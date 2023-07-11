import { TreeDbRecord, TreeRecord } from './model';
import { Client } from 'pg';
import * as dotenv from 'dotenv';
import * as process from 'process';
import { MagdeburgLoadingStrategy } from './loading-strategies/magdeburg-loading-strategy';
import { TestLoadingStrategy } from './loading-strategies/test-loading-strategy';
import { ILoadingStrategy } from './loading-strategies/loading-strategy';
import { GoogleSheetLoadingStrategy } from './loading-strategies/google-sheet-loading-strategy';
import { GENUS_MAP } from './genera';


dotenv.config();


type TreeDataComparisonResult = {
    deletedTrees: TreeDbRecord[];
    updatedTrees: TreeDbRecord[];
    addedTrees: TreeDbRecord[];
};


type StrategyKey = 'magdeburg' | 'test' | 'google-sheet';


const LOADING_STRATEGIES: Map<StrategyKey, ILoadingStrategy> = new Map(
    [
        ['magdeburg', new MagdeburgLoadingStrategy()],
        ['test', new TestLoadingStrategy()],
        ['google-sheet', new GoogleSheetLoadingStrategy()]
    ]
);


function loadTreeData(strategyKey: StrategyKey, importingOptions: string[]): Promise<TreeRecord[]> {

    const strategy = LOADING_STRATEGIES.get(strategyKey);
    if (!strategy) {
        throw new Error(`Unknown strategy ${strategyKey}`);
    }
    return strategy.load(importingOptions);

}


function convertTreeData(trees: TreeRecord[], source: string): TreeDbRecord[] {

    return trees.map(tree => {

        const genusDescription = GENUS_MAP.get(tree.genus);
        if (!genusDescription) {
            console.warn(`No genus description found for genus "${tree.genus}" (tree ${tree.ref})`);
        }

        return {
            id: tree.internal_ref,
            lat: `${tree.lat}`,
            lng: `${tree.lon}`,
            artdtsch: tree.common,
            artbot: tree.species,
            gattungdeutsch: genusDescription ? genusDescription.displayName : null,
            gattung: tree.genus,
            strname: tree.address,
            kronedurch: `${tree.crown}`,
            stammumfg: `${Math.round(tree.dbh * Math.PI)}`,
            baumhoehe: `${tree.height}`,
            geom: `SRID=4326;POINT(${tree.lon} ${tree.lat})`,
            pflanzjahr: tree.planted,
            gmlid: tree.ref,
            source
        };
    });
}


async function readOldTreeData(dbClient: Client, source: string): Promise<TreeDbRecord[]> {
    const { rows } = await dbClient.query(
        'select * from trees where source = $1',
        [source]
    );
    return rows;
}


async function compareTreeData(newTrees: TreeDbRecord[], oldTrees: TreeDbRecord[]): Promise<TreeDataComparisonResult> {

    const treesAreSame = (tree1: TreeDbRecord, tree2: TreeDbRecord): boolean =>
        tree1.gmlid === tree2.gmlid && tree1.source === tree2.source
    const propertiesAreDifferent = (tree1: TreeDbRecord, tree2: TreeDbRecord): boolean =>
        tree1.lat !== tree2.lat
        || tree1.lng !== tree2.lng
        || tree1.artdtsch !== tree2.artdtsch
        || tree1.artbot !== tree2.artbot
        || tree1.gattungdeutsch !== tree2.gattungdeutsch
        || tree1.gattung !== tree2.gattung
        || tree1.strname !== tree2.strname
        || tree1.kronedurch !== tree2.kronedurch
        || tree1.stammumfg !== tree2.stammumfg
        || tree1.baumhoehe !== tree2.baumhoehe
        || tree1.pflanzjahr !== tree2.pflanzjahr
        //|| tree1.geom !== tree2.geom
    ;

    console.log('Start finding deleted trees: ', new Date().toISOString());
    const deletedTrees = oldTrees.filter(
        oldTree => !newTrees.some(newTree => treesAreSame(oldTree, newTree))
    );

    console.log('Start finding changed trees: ', new Date().toISOString());
    const updatedTrees = newTrees.filter(newTree =>
        oldTrees.some(oldTree =>
            treesAreSame(oldTree, newTree) && propertiesAreDifferent(oldTree, newTree)
        )
    );

    console.log('Start finding new trees: ', new Date().toISOString());
    const addedTrees = newTrees.filter(
        newTree => !oldTrees.some(oldTree => treesAreSame(oldTree, newTree))
    );

    console.log('Done: ', new Date().toISOString());

    return {
        deletedTrees,
        updatedTrees,
        addedTrees
    };

}


async function deleteFromDb(dbClient: Client, trees: TreeDbRecord[]) {

    await dbClient.query(`
        drop table if exists deleted_trees_tmp;
        create table deleted_trees_tmp
        (
            id text not null primary key
        );
    `);

    await dbClient.query(`
        insert into deleted_trees_tmp (id)
        select id from json_populate_recordset(null::deleted_trees_tmp, $1::JSON);
    `, [JSON.stringify(trees.map(tree => ({ id: tree.id })))]);

    await dbClient.query(`
        delete from trees_adopted
        where tree_id in (select id from deleted_trees_tmp)
    `);

    await dbClient.query(`
        delete from trees_watered
        where tree_id in (select id from deleted_trees_tmp)
    `);

    await dbClient.query(`
        delete from trees
        where id in (select id from deleted_trees_tmp);
    `);

    await dbClient.query('drop table deleted_trees_tmp;');

}


async function updateDb(dbClient: Client, trees: TreeDbRecord[]) {

    await dbClient.query(`
        drop table if exists updated_trees_tmp;
        create table updated_trees_tmp
        (
            id text not null primary key,
            lat text,
            lng text,
            artdtsch text,
            artbot text,
            gattungdeutsch text,
            gattung text,
            strname text,
            kronedurch text,
            stammumfg text,
            baumhoehe text,
            pflanzjahr integer,
            geom geometry(Point, 4326),
            gmlid text,
            source text
        );
    `);

    await dbClient.query(`
        insert into updated_trees_tmp (id, lat, lng, artdtsch, artbot, gattungdeutsch, gattung, strname, kronedurch, stammumfg,
                           baumhoehe, pflanzjahr, geom, gmlid, source)
        select id,
               lat,
               lng,
               artdtsch,
               artbot,
               gattungdeutsch,
               gattung,
               strname,
               kronedurch,
               stammumfg,
               baumhoehe,
               pflanzjahr,
               geom,
               gmlid,
               source
        from json_populate_recordset(null::trees, $1::JSON)
    `, [JSON.stringify(trees)]);

    await dbClient.query(`
        update trees
        set
            lat = updated_trees_tmp.lat, 
            lng = updated_trees_tmp.lng,
            artdtsch = updated_trees_tmp.artdtsch,
            artbot = updated_trees_tmp.artbot,
            gattungdeutsch = updated_trees_tmp.gattungdeutsch,
            gattung = updated_trees_tmp.gattung,
            strname = updated_trees_tmp.strname,
            kronedurch = updated_trees_tmp.kronedurch,
            stammumfg = updated_trees_tmp.stammumfg,
            baumhoehe = updated_trees_tmp.baumhoehe,
            pflanzjahr = updated_trees_tmp.pflanzjahr,
            geom = updated_trees_tmp.geom
        from updated_trees_tmp
        where updated_trees_tmp.gmlid = trees.gmlid and updated_trees_tmp.source = trees.source
    `);

    await dbClient.query('drop table updated_trees_tmp;');

}


async function addToDb(dbClient: Client, trees: TreeDbRecord[]) {

    await dbClient.query(`
        insert into trees (id, lat, lng, artdtsch, artbot, gattungdeutsch, gattung, strname, kronedurch, stammumfg,
                           baumhoehe, pflanzjahr, geom, gmlid, source)
        select id,
               lat,
               lng,
               artdtsch,
               artbot,
               gattungdeutsch,
               gattung,
               strname,
               kronedurch,
               stammumfg,
               baumhoehe,
               pflanzjahr,
               geom,
               gmlid,
               source
        from json_populate_recordset(null::trees, $1::JSON)
    `, [JSON.stringify(trees)]);

}


async function run(source: string, importingStrategy: StrategyKey, importingOptions: string[]): Promise<TreeDataComparisonResult> {

    const loadedNewTreeData = await loadTreeData(importingStrategy, importingOptions);
    const convertedNewTreeData = convertTreeData(loadedNewTreeData, source);

    const dbClient = new Client({
        user: process.env.PG_USER,
        password: process.env.PG_PASSWORD,
        host: process.env.PG_HOST,
        database: process.env.PG_DATABASE,
        port: +process.env.PG_PORT
    });

    await dbClient.connect();

    const oldTreeData = await readOldTreeData(dbClient, source);
    const comparisonResult = await compareTreeData(convertedNewTreeData, oldTreeData);

    await deleteFromDb(dbClient, comparisonResult.deletedTrees);
    await updateDb(dbClient, comparisonResult.updatedTrees);
    await addToDb(dbClient, comparisonResult.addedTrees);

    await dbClient.end();

    return comparisonResult;

}


//run('ls', 'magdeburg', ['2022', './data/2022/2022_Liegenschaftsservice.csv'])
//run('sfm', 'magdeburg', ['2022', './data/2022/2022_SFM.csv'])
//run('ls', 'magdeburg', ['2023', './data/2023/2023_Liegenschaftsservice.csv'])
//run('sfm', 'magdeburg', ['2023', './data/2023/2023_SFM.csv'])

run('test', 'test', [])

// run(
//     'test-google-sheet',
//     'google-sheet',
//     ['https://docs.google.com/spreadsheets/d/e/2PACX-1vSZCQj6Ph4kibmaNQaJF2alcHw3c5lcdqHbR8DVPyaBR861THe7UrJjiJMppgL0LIif8xUgcadFJ-6M/pub?gid=0&single=true&output=csv']
// )

// run(
//     'otto-pflanzt',
//     'google-sheet',
//     ['https://docs.google.com/spreadsheets/d/e/2PACX-1vQBkh8OA6UJ2DgUiHNN71x_z2O0ZjVnBNZ5_nvmAaS1VfshzpV26zpCMH4IUQ_yes20TcCMvxUzoUzx/pub?gid=0&single=true&output=csv']
// )

    .then(result => {
        console.log(`Deleted: ${result.deletedTrees.length}`);
        console.log(`Updated: ${result.updatedTrees.length}`);
        console.log(`Added: ${result.addedTrees.length}`);
        console.log('Done.');
    })
    .catch(console.error);
