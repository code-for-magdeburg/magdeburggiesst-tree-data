import { TreeRecord } from '../model';

export interface ILoadingStrategy {
    load(loadingOptions: string[]): Promise<TreeRecord[]>;
}
