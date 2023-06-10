import { ILoadingStrategy } from './loading-strategy';
import { TreeRecord } from '../model';
import { readMagdeburg2022 } from '../read-magdeburg-2022';
import { readMagdeburg2023 } from '../read-magdeburg-2023';

export class MagdeburgLoadingStrategy implements ILoadingStrategy {

    load(loadingOptions: string[]): Promise<TreeRecord[]> {

        const version = loadingOptions[0];
        switch (version) {
            case '2022':
                return Promise.resolve(readMagdeburg2022(loadingOptions[1]));
            case '2023':
                return Promise.resolve(readMagdeburg2023(loadingOptions[1]));

            default:
                throw new Error(`Unknown version ${version}`);
        }

    }
}
