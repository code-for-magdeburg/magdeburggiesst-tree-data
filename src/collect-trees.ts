import { merge2022 } from './merge-2022';
import { TreeRecord } from './model';


export async function collectTrees(): Promise<TreeRecord[]> {

    const allTrees: TreeRecord[] = [];

    // Merging 2019-2021 is not implemented. We will start with using data from year 2022

    //await merge2019(allTrees);
    //await merge2020(allTrees);
    //await merge2021(allTrees);

    await merge2022(allTrees);

    return allTrees;

}
