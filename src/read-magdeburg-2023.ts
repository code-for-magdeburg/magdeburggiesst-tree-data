import { TreeRecord } from './model';
import * as fs from 'fs';
import { parse, ParseConfig } from 'papaparse';
import { nanoid } from 'nanoid';


type OriginalTree2023CsvRecord = {
    fid: number;
    Baumnummer: string;
    Hoehe: number;
    Gattung: string;
    gebiet: string;
    Kronendurchm: number;
    pflanzjahr: number;
    strasse: string;
    Stammumfang: number;
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
    ['Ostrya carpinifolia - Hopfenbuche', 'Ostrya carpinifolia, Hopfenbuche'],
    ['unbekannt', null],
    ['Unbekannt', null],
    ['waldartiger Bestand', null],
    ['Leerstelle', null],
    ['Baumgruppe', null]
]);


export function readMagdeburg2023(inputCsvFile: string): TreeRecord[] {

    const loadedTrees = loadTrees2023(inputCsvFile);
    const validTrees = filterInvalidTrees(loadedTrees);
    const fixedTrees = fixTrees(validTrees);
    return transformTrees(fixedTrees);

}


function loadTrees2023(filename: string): OriginalTree2023CsvRecord[] {
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
                case 'Stammumfang':
                    return value ? parseInt(value, 10) : null;
                case 'pflanzjahr':
                    return value ? parseInt(value, 10) : null;
                default:
                    return value;
            }
        }
    };
    return parse(csv, parseOptions).data as OriginalTree2023CsvRecord[];
}


function filterInvalidTrees(trees: OriginalTree2023CsvRecord[]): OriginalTree2023CsvRecord[] {

    const hashId = (tree: OriginalTree2023CsvRecord): string => `${tree.gebiet}${tree.Baumnummer}`;

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
            && tree.Gattung !== 'Leerstelle'
            && cntIds[hashId(tree)] === 1
    );

}


function fixTrees(trees: OriginalTree2023CsvRecord[]): OriginalTree2023CsvRecord[] {

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


function transformTrees(trees: OriginalTree2023CsvRecord[]): TreeRecord[] {

    return trees.map(tree => {

        const id = createTreeRef(tree);
        return {
            internal_ref: nanoid(),
            ref: id,
            location: tree.gebiet,
            address: tree.strasse,
            lat: tree.latitude,
            lon: tree.longitude,
            genus: tree.Gattung,
            height: tree.Hoehe,
            crown: tree.Kronendurchm,
            dbh: tree.Stammumfang / Math.PI,
            planted: tree.pflanzjahr
        };

    });

}


function createTreeRef(tree: OriginalTree2023CsvRecord): string {

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
