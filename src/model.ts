
// See http://standards.opencouncildata.org/#/trees
export type TreeRecord = {
    internal_ref: string;
    ref: string;
    location: string;
    address: string;
    lat: number;
    lon: number;
    genus: string;
    species: string;
    common: string;
    height: number;
    crown: number;
    dbh: number;
    planted: number;
};


export type TreeDbRecord = {
    id: string;
    lat: string;
    lng: string;
    artdtsch: string;
    artbot: string;
    gattungdeutsch: string;
    gattung: string;
    strname: string;
    kronedurch: string;
    stammumfg: string;
    baumhoehe: string;
    geom: string;
    pflanzjahr: number;
    gmlid: string;
    source: string;
};


export type TreeClassification = {
    fullname: string;
    genus: string;
    species: string;
    variety: string;
    common: string;
    scientific: string;
};
