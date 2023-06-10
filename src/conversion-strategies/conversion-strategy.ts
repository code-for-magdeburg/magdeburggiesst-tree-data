import { TreeDbRecord, TreeRecord } from '../model';


export interface IConversionStrategy {
    convertTreeData(trees: TreeRecord[], source: string): TreeDbRecord[];
}
