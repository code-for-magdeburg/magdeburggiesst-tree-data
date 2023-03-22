import { collectTrees } from './collect-trees';
import { TreeDbRecord, TreeRecord } from './model';
import { Client } from 'pg';
import * as dotenv from 'dotenv';
import * as process from 'process';


dotenv.config();


type TreeDataComparisonResult = {
    deletedTrees: TreeDbRecord[];
    updatedTrees: TreeRecord[];
    addedTrees: TreeRecord[];
};


async function readOldTreeData(dbClient: Client): Promise<TreeDbRecord[]> {
    const { rows } = await dbClient.query('select * from trees');
    return rows;
}


async function compareTreeData(newTrees: TreeRecord[], oldTrees: TreeDbRecord[]): Promise<TreeDataComparisonResult> {

    const deletedTrees = oldTrees.filter(oldTree => !newTrees.some(newTree => newTree.ref === oldTree.gmlid));
    const updatedTrees = newTrees.filter((newTree, index) =>
        oldTrees.some(oldTree => oldTree.gmlid === newTree.ref)
    );
    const addedTrees = newTrees.filter(newTree => !oldTrees.some(oldTree => oldTree.gmlid === newTree.ref));

    return {
        deletedTrees,
        updatedTrees,
        addedTrees
    };

}


function mapToDbRecord(tree: TreeRecord): TreeDbRecord {
    return {
        id: tree.internal_ref,
        lat: `${tree.lat}`,
        lng: `${tree.lon}`,
        artdtsch: tree.genus, // TODO
        artbot: tree.genus, // TODO
        gattungdeutsch: tree.genus, // TODO
        gattung: tree.genus, // TODO
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


async function updateDb(dbClient: Client, trees: TreeRecord[]) {

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

    const records = trees.map(mapToDbRecord);

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
    `, [JSON.stringify(records)]);

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
        where updated_trees_tmp.id = trees.id
    `);

    await dbClient.query('drop table updated_trees_tmp;');

}


async function addToDb(dbClient: Client, trees: TreeRecord[]) {

    const records = trees.map(mapToDbRecord);

    await dbClient.query(`
        insert into trees (id, lat, lng, artdtsch, artbot, gattungdeutsch, gattung, strname, kronedurch, stammumfg,
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
    `, [JSON.stringify(records)]);

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
    const newTreeData = await collectTrees();
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
