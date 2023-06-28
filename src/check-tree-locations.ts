import { Client } from 'pg';
import * as dotenv from 'dotenv';
import * as process from 'process';
import { TreeDbRecord } from './model';
import * as turf from '@turf/turf';
import * as fs from 'fs';
import { Feature } from '@turf/turf';


dotenv.config();


type DbTreeWithPoint = TreeDbRecord & { point: Feature<turf.Point> };

type OsmTree = {
    nodeId: number;
    point: Feature<turf.Point>;
}


function loadOsmTreesJson(filename): OsmTree[] {

    const json = JSON.parse(fs.readFileSync(filename, 'utf8'));
    return json.elements.map(node => ({
        ...node,
        point: turf.point([node.lon, node.lat])
    }));

}


async function readTreeData(): Promise<DbTreeWithPoint[]> {

    const dbClient = new Client({
        user: process.env.PG_USER,
        password: process.env.PG_PASSWORD,
        host: process.env.PG_HOST,
        database: process.env.PG_DATABASE,
        port: +process.env.PG_PORT
    });

    await dbClient.connect();

    const { rows } = await dbClient.query<TreeDbRecord>(
        'select * from trees where source != \'osm\''
    );

    await dbClient.end();

    return rows.map(tree => ({
        ...tree,
        point: turf.point([+tree.lng, +tree.lat])
    }));

}


async function run() {

    const osmTrees = loadOsmTreesJson('./data/OpenStreetMap/osm-trees-2023-06-11.json');
    const currentTrees = await readTreeData();

    console.log('Starting to check trees.');

    const candidates = osmTrees
        .map((tree, index) => {

            console.log(`Checking tree ${index} of ${osmTrees.length}`);

            const distances = currentTrees
                .map(otherTree => +turf.distance(tree.point, otherTree.point, { units: 'meters' }))
                .sort((a, b) => a - b);

            return {
                ...tree,
                metersToNearestTree: distances[0]
            };

        })
        //.filter(tree => tree.metersToNearestTree > 0 && tree.metersToNearestTree < 2);

    fs.writeFileSync('./data/OpenStreetMap/candidates-all.json', JSON.stringify(candidates, null, 2));

    console.log('Done.');

}


run()
    .then(result => {
        console.log(result);
    })
    .catch(console.error);

