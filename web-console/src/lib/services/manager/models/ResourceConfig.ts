/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export type ResourceConfig = {
  /**
   * The maximum number of CPU cores to reserve
   * for an instance of this pipeline
   */
  cpu_cores_max?: number | null
  /**
   * The minimum number of CPU cores to reserve
   * for an instance of this pipeline
   */
  cpu_cores_min?: number | null
  /**
   * The maximum memory in Megabytes to reserve
   * for an instance of this pipeline
   */
  memory_mb_max?: number | null
  /**
   * The minimum memory in Megabytes to reserve
   * for an instance of this pipeline
   */
  memory_mb_min?: number | null
  /**
   * The total storage in Megabytes to reserve
   * for an instance of this pipeline
   */
  storage_mb_max?: number | null
}
