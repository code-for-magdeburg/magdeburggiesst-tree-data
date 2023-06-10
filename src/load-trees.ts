import { readMagdeburg2022 } from './read-magdeburg-2022';
import { readMagdeburg2023 } from './read-magdeburg-2023';
import { TreeRecord } from './model';
import { nanoid } from 'nanoid';


function readTest(): TreeRecord[] {
    return [
        {
            internal_ref: nanoid(),
            ref: '1',
            location: 'Testgebiet',
            address: 'Teststraße',
            lat: 52.123,
            lon: 11.123,
            genus: 'Sorbus aucuparia, Eberesche (Vogelbeere)',
            height: 10,
            crown: 10,
            dbh: 10,
            planted: 2020
        },
        {
            internal_ref: nanoid(),
            ref: '2',
            location: 'Testgebiet',
            address: 'Teststraße',
            lat: 52.124,
            lon: 11.124,
            genus: 'Sorbus aucuparia, Eberesche (Vogelbeere)',
            height: 10,
            crown: 10,
            dbh: 10,
            planted: 2020
        },
        {
            internal_ref: nanoid(),
            ref: '3',
            location: 'Testgebiet',
            address: 'Teststraße',
            lat: 52.122,
            lon: 11.122,
            genus: 'Sorbus aucuparia, Eberesche (Vogelbeere)',
            height: 10,
            crown: 10,
            dbh: 10,
            planted: 2020
        },
    ];
}

export function loadTrees(loadingStrategy: string, inputCsvFile: string): TreeRecord[] {

    switch (loadingStrategy) {

        case 'magdeburg-2022':
            return readMagdeburg2022(inputCsvFile);

        case 'magdeburg-2023':
            return readMagdeburg2023(inputCsvFile);

        case 'test':
            return readTest();

        default:
            throw new Error(`Unknown load type: ${loadingStrategy}`);

    }

}
