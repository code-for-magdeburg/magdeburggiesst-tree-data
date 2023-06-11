import { ILoadingStrategy } from './loading-strategy';
import { TreeRecord } from '../model';
import { nanoid } from 'nanoid';


export class TestLoadingStrategy implements ILoadingStrategy {

    load(loadingOptions: string[]): Promise<TreeRecord[]> {
        return Promise.resolve([
            {
                internal_ref: nanoid(),
                ref: '1',
                location: 'Testgebiet',
                address: 'Teststraße',
                lat: 52.123,
                lon: 11.123,
                genus: 'Sorbus',
                species: 'Sorbus aucuparia',
                common: 'Eberesche (Vogelbeere)',
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
                genus: 'Sorbus',
                species: 'Sorbus aucuparia',
                common: 'Eberesche (Vogelbeere)',
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
                genus: 'Sorbus',
                species: 'Sorbus aucuparia',
                common: 'Eberesche (Vogelbeere)',
                height: 10,
                crown: 10,
                dbh: 10,
                planted: 2020
            },
        ]);
    }

}
