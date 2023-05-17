import { merge2023 } from './merge-2023';
import { TreeRecord } from './model';


export async function collectTrees(): Promise<TreeRecord[]> {

    const allTrees: TreeRecord[] = [];

    // Merging 2019-2022 is not implemented. We will start with using data from year 2023

    //await merge2019(allTrees);
    //await merge2020(allTrees);
    //await merge2021(allTrees);
    //await merge2022(allTrees);

    await merge2023(allTrees);

    return allTrees;

}
