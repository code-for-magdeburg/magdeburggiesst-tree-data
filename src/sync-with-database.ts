import { collectTrees } from './collect-trees';
import { TreeClassification, TreeDbRecord, TreeRecord } from './model';
import { Client } from 'pg';
import * as dotenv from 'dotenv';
import * as process from 'process';
import { GENUS_MAP } from './genera';
import { parse } from 'papaparse';


dotenv.config();


type TreeDataComparisonResult = {
    deletedTrees: TreeDbRecord[];
    updatedTrees: TreeDbRecord[];
    addedTrees: TreeDbRecord[];
};


async function readOldTreeData(dbClient: Client): Promise<TreeDbRecord[]> {
    const { rows } = await dbClient.query('select * from trees');
    return rows;
}


async function compareTreeData(newTrees: TreeDbRecord[], oldTrees: TreeDbRecord[]): Promise<TreeDataComparisonResult> {

    const treesAreSame = (tree1: TreeDbRecord, tree2: TreeDbRecord): boolean => tree1.gmlid === tree2.gmlid;
    const treesAreEqual = (tree1: TreeDbRecord, tree2: TreeDbRecord): boolean =>
        tree1.lat !== tree2.lng
        || tree1.lng !== tree2.lat
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
        oldTrees.some(oldTree => treesAreSame(oldTree, newTree) && treesAreEqual(oldTree, newTree))
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


function mapToDbRecord(tree: TreeRecord): TreeDbRecord {
    const classification = mapToClassification(tree.genus);
    const genus = GENUS_MAP.get(classification.genus);
    if (!genus) {
        console.warn(`Classification for ${tree.internal_ref} not found. Genus: ${tree.genus}`);
    }
    return {
        id: tree.internal_ref,
        lat: `${tree.lat}`,
        lng: `${tree.lon}`,
        artdtsch: classification.common,
        artbot: classification.scientific,
        gattungdeutsch: genus ? genus.displayName : null,
        gattung: classification.genus,
        strname: tree.address,
        kronedurch: `${tree.crown}`,
        stammumfg: `${Math.round(tree.dbh * Math.PI)}`,
        baumhoehe: `${tree.height}`,
        geom: `SRID=4326;POINT(${tree.lon} ${tree.lat})`,
        pflanzjahr: tree.planted,
        gmlid: tree.ref
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
        delete from trees_adopted
        where tree_id in (select id from deleted_trees_tmp)
    `);

    await dbClient.query(`
        delete from trees_watered
        where tree_id in (select id from deleted_trees_tmp)
    `);

    await dbClient.query(`
        insert into deleted_trees_tmp (id)
        select id from json_populate_recordset(null::deleted_trees_tmp, $1::JSON);
    `, [JSON.stringify(trees.map(tree => ({ id: tree.id })))]);

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
            gmlid text
        );
    `);

    await dbClient.query(`
        insert into updated_trees_tmp (id, lat, lng, artdtsch, artbot, gattungdeutsch, gattung, strname, kronedurch, stammumfg,
                           baumhoehe, pflanzjahr, geom, gmlid)
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
               gmlid
        from json_populate_recordset(null::trees, $1::JSON)
    `, [JSON.stringify(trees)]);

    await dbClient.query(`
        update trees
        set
            lat = updated_trees_tmp.lng, -- HINT: latitude and longitude are swapped on purpose 
            lng = updated_trees_tmp.lat, -- due to a known bug in GdK sources: https://github.com/technologiestiftung/giessdenkiez-de-postgres-api/issues/67
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
        where updated_trees_tmp.gmlid = trees.gmlid
    `);

    await dbClient.query('drop table updated_trees_tmp;');

}


async function addToDb(dbClient: Client, trees: TreeDbRecord[]) {

    await dbClient.query(`
        insert into trees (id, lat, lng, artdtsch, artbot, gattungdeutsch, gattung, strname, kronedurch, stammumfg,
                           baumhoehe, pflanzjahr, geom, gmlid)
        select id,
               lng, -- HINT: latitude and longitude are swapped on purpose
               lat, -- due to a known bug in GdK sources: https://github.com/technologiestiftung/giessdenkiez-de-postgres-api/issues/67
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
               gmlid
        from json_populate_recordset(null::trees, $1::JSON)
    `, [JSON.stringify(trees)]);

}


function mapToClassification(input: string): TreeClassification {

    if (!input) {
        return { fullname: null, genus: null, species: null, variety: null, scientific: null, common: null };
    }

    const parts = input.split(',');
    const scientific = parts.length > 0 ? parts[0].trim() : '';
    const common = parts.length > 1 ? parts[1].trim() : scientific;

    const scientificParts = (parse(scientific, { delimiter: ' ', quoteChar: '"' }).data)[0] as string[];
    const genus = scientificParts[0];
    const species = scientificParts[1].toLowerCase() === 'x'
        ? `x ${scientificParts[2]}`
        : scientificParts[1];
    const variety = scientificParts[1].toLowerCase() === 'x'
        ? scientificParts.slice(3).join(' ')
        : scientificParts.slice(2).join(' ');

    return { fullname: input, genus, species, variety, scientific, common };

}


(async () => {

    const dbClient = new Client({
        user: process.env.PG_USER,
        password: process.env.PG_PASSWORD,
        host: process.env.PG_HOST,
        database: process.env.PG_DATABASE,
        port: +process.env.PG_PORT
    });

    await dbClient.connect();

    const oldTreeData = await readOldTreeData(dbClient);
    const newTreeData = (await collectTrees()).map(mapToDbRecord);
    const { updatedTrees, deletedTrees, addedTrees } = await compareTreeData(newTreeData, oldTreeData);

    await deleteFromDb(dbClient, deletedTrees);
    await updateDb(dbClient, updatedTrees);
    await addToDb(dbClient, addedTrees);

    await dbClient.end();

    console.log(`Deleted: ${deletedTrees.length}`);
    console.log(`Updated: ${updatedTrees.length}`);
    console.log(`Added: ${addedTrees.length}`);
    console.log('Done.');

})();
