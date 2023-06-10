import { IConversionStrategy } from './conversion-strategy';
import { TreeDbRecord, TreeRecord } from '../model';


export class TestConversionStrategy implements IConversionStrategy {

    convertTreeData(trees: TreeRecord[], source: string): TreeDbRecord[] {

        return trees.map(tree => ({
            id: tree.internal_ref,
            lat: `${tree.lat}`,
            lng: `${tree.lon}`,
            artdtsch: tree.genus,
            artbot: tree.genus,
            gattungdeutsch: tree.genus,
            gattung: tree.genus,
            strname: tree.address,
            kronedurch: `${tree.crown}`,
            stammumfg: `${Math.round(tree.dbh * Math.PI)}`,
            baumhoehe: `${tree.height}`,
            geom: `SRID=4326;POINT(${tree.lon} ${tree.lat})`,
            pflanzjahr: tree.planted,
            gmlid: tree.ref,
            source
        }));

    }

}
