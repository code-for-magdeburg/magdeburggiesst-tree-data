import { TreeClassification, TreeRecord } from './model';
import * as fs from 'fs';
import { parse, ParseConfig } from 'papaparse';
import { nanoid } from 'nanoid';


type OriginalTree2022CsvRecord = {
    fid: number;
    Baumnummer: string;
    Hoehe: number;
    Gattung: string;
    gebiet: string;
    Kronendurchm: number;
    pflanzjahr: number;
    strasse: string;
    Stammdurchm: number;
    longitude: number;
    latitude: number;
};


const IGNORED_ADDRESS_WORDS = [
    'Bäume - Liegenschaftsservice',
    '/KGA',
    '/LSG',
    '/PPL',
    '/SBG',
    '/SF',
    '/SP',
];


const GENUS_TRANSLATE_MAP = new Map([
    ['Tilia europaea "Pallida"; Kaiser-Linde', 'Tilia europaea "Pallida", Kaiser-Linde'],
    ['Ostrya carpinifolia - Hopfenbuche', 'Ostrya carpinifolia, Hopfenbuche'],
    ['unbekannt', null],
    ['waldartiger Bestand', null]
]);


export function readMagdeburg2022(inputCsvFile: string): TreeRecord[] {

    const loadedTrees = loadTrees2022(inputCsvFile);
    const validTrees = filterInvalidTrees(loadedTrees);
    const fixedTrees = fixTrees(validTrees);

    return transformTrees(fixedTrees);

}


function loadTrees2022(filename: string): OriginalTree2022CsvRecord[] {
    const csv = fs.readFileSync(filename, 'utf-8');
    const parseOptions: ParseConfig = {
        skipEmptyLines: true,
        header: true,
        transform: (value: string, field: string | number): any => {
            switch (field) {
                case 'Hoehe':
                    return value ? parseFloat(value.replace(',', '.')) : null;
                case 'Kronendurchm':
                    return value ? parseFloat(value) : null;
                case 'Stammdurchm':
                    return value ? parseInt(value, 10) : null;
                case 'pflanzjahr':
                    return value ? parseInt(value, 10) : null;
                default:
                    return value;
            }
        }
    };
    return parse(csv, parseOptions).data as OriginalTree2022CsvRecord[];
}


function filterInvalidTrees(trees: OriginalTree2022CsvRecord[]): OriginalTree2022CsvRecord[] {

    const hashId = (tree: OriginalTree2022CsvRecord): string => `${tree.gebiet}${tree.Baumnummer}`;

    const cntIds = trees
        .map(hashId)
        .sort()
        .reduce((p, c) => {
            p[c] = (p[c] || 0) + 1;
            return p;
        }, {});

    return trees.filter(
        tree =>
            tree.Baumnummer
            && tree.Baumnummer.length >= 3
            && tree.strasse !== 'Testgebiet'
            && cntIds[hashId(tree)] === 1
    );

}


function fixTrees(trees: OriginalTree2022CsvRecord[]): OriginalTree2022CsvRecord[] {

    return trees.map(tree => {
        return {
            ...tree,
            strasse: fixStrasse(tree.strasse),
            Gattung: fixGattung(tree.Gattung),
            pflanzjahr: fixPflanzjahr(tree.pflanzjahr)
        };
    });

}


function fixStrasse(strasse: string): string {
    let resultStrasse = strasse;
    IGNORED_ADDRESS_WORDS.forEach(word => resultStrasse = resultStrasse.replace(word, '').trim());
    return resultStrasse;
}


function fixGattung(gattung: string): string {
    return GENUS_TRANSLATE_MAP.has(gattung) ? GENUS_TRANSLATE_MAP.get(gattung) : gattung;
}


function fixPflanzjahr(pflanzjahr: number): number {
    return pflanzjahr < 1600 ? null : pflanzjahr;
}


function transformTrees(trees: OriginalTree2022CsvRecord[]): TreeRecord[] {

    return trees.map(tree => {

        const classification = mapToClassification(tree.Gattung);
        return {
            internal_ref: nanoid(),
            ref: createTreeRef(tree),
            location: tree.gebiet,
            address: tree.strasse,
            lat: tree.latitude,
            lon: tree.longitude,
            genus: classification.genus,
            species: classification.scientific,
            common: classification.common,
            height: tree.Hoehe,
            crown: tree.Kronendurchm,
            dbh: tree.Stammdurchm,
            planted: tree.pflanzjahr
        };

    });

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


function createTreeRef(tree: OriginalTree2022CsvRecord): string {

    let key = '';
    switch (tree.gebiet) {
        case 'Öffentliches Grün':
            key = 'G';
            break;

        case 'AMT 66':
            key = 'S';
            break;

        case 'Spielplatz':
            key = 'K';
            break;

        case 'Liegenschaftsservice':
            key = 'L';
            break;

        default:
            throw Error(`Could not create tree ref (Baunummer=${tree.Baumnummer}, gebiet=${tree.gebiet})`);
    }

    return `${key}${tree.Baumnummer}`;

}
