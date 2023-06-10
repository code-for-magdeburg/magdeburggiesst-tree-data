import { IConversionStrategy } from './conversion-strategy';
import { TreeClassification, TreeDbRecord, TreeRecord } from '../model';
import { GENUS_MAP } from '../genera';
import { parse } from 'papaparse';


export class MagdeburgConversionStrategy implements IConversionStrategy {

    convertTreeData(trees: TreeRecord[], source: string): TreeDbRecord[] {
        return trees.map(tree => this.mapToDbRecord(tree, source));
    }


    private mapToDbRecord(tree: TreeRecord, source: string): TreeDbRecord {
        const classification = this.mapToClassification(tree.genus);
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
            gmlid: tree.ref,
            source
        };
    }


    private mapToClassification(input: string): TreeClassification {

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


}
