// Zips two lists together.
//
// console.log( zip([1,2,3], ["a","b","c","d"]) );
// [[1, "a"], [2, "b"], [3, "c"], [undefined, "d"]]
//
// See: https://stackoverflow.com/questions/22015684/zip-arrays-in-javascript
export const zip = (a: any, b: any) => Array.from(Array(Math.max(b.length, a.length)), (_, i) => [a[i], b[i]])
