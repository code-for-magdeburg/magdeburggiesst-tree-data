import { ILoadingStrategy } from './loading-strategy';
import { TreeRecord } from '../model';
import axios from 'axios';
import * as Papa from 'papaparse';
import { nanoid } from 'nanoid';


export class GoogleSheetLoadingStrategy implements ILoadingStrategy {

    async load(loadingOptions: string[]): Promise<TreeRecord[]> {

        return new Promise((resolve, reject) =>
            axios
                .get(loadingOptions[0])
                .then(response => {

                    const csvData = response.data;
                    const results = Papa.parse(csvData, { header: true }).data as any[];

                    resolve(results.map(result => ({
                        internal_ref: nanoid(),
                        ref: result.Id,
                        location: null,
                        address: result.Adresse,
                        lat: parseFloat(result.Latitude.replace(',', '.')),
                        lon: parseFloat(result.Longitude.replace(',', '.')),
                        genus: result.Gattung,
                        species: result.Art,
                        common: result.AllgemeinerName,
                        height: parseFloat(result.Baumhoehe.replace(',', '.')),
                        crown: parseFloat(result.Kronendurchmesser.replace(',', '.')),
                        dbh: Math.round(parseFloat(result.Stammumfang.replace(',', '.')) / Math.PI),
                        planted: parseInt(result.Pflanzjahr, 10)
                    })));

                })
                .catch(reject));

    }

}
