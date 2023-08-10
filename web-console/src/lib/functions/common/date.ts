// This file contains some utility functions used throughout the project.

/// Returns a random date between `start` and `end`.
//
// The maximum is exclusive and the minimum is inclusive.
export function getRandomDate(start: Date, end: Date): Date {
  return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()))
}
