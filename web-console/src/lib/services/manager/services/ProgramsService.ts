/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { CompileProgramRequest } from '../models/CompileProgramRequest'
import type { NewProgramRequest } from '../models/NewProgramRequest'
import type { NewProgramResponse } from '../models/NewProgramResponse'
import type { ProgramDescr } from '../models/ProgramDescr'
import type { UpdateProgramRequest } from '../models/UpdateProgramRequest'
import type { UpdateProgramResponse } from '../models/UpdateProgramResponse'

import type { CancelablePromise } from '../core/CancelablePromise'
import { OpenAPI } from '../core/OpenAPI'
import { request as __request } from '../core/request'

export class ProgramsService {
  /**
   * Fetch programs, optionally filtered by name or ID.
   * Fetch programs, optionally filtered by name or ID.
   * @param id Unique program identifier.
   * @param name Unique program name.
   * @param withCode Option to include the SQL program code or not
   * in the Program objects returned by the query.
   * If false (default), the returned program object
   * will not include the code.
   * @returns ProgramDescr Programs retrieved successfully.
   * @throws ApiError
   */
  public static getPrograms(
    id?: string | null,
    name?: string | null,
    withCode?: boolean | null
  ): CancelablePromise<Array<ProgramDescr>> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/programs',
      query: {
        id: id,
        name: name,
        with_code: withCode
      },
      errors: {
        404: `Specified program name or ID does not exist.`
      }
    })
  }

  /**
   * Create a new program.
   * Create a new program.
   * @param requestBody
   * @returns NewProgramResponse Program created successfully
   * @throws ApiError
   */
  public static newProgram(requestBody: NewProgramRequest): CancelablePromise<NewProgramResponse> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/programs',
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        409: `A program with this name already exists in the database.`
      }
    })
  }

  /**
   * Fetch a program by ID.
   * Fetch a program by ID.
   * @param programId Unique program identifier
   * @param withCode Option to include the SQL program code or not
   * in the Program objects returned by the query.
   * If false (default), the returned program object
   * will not include the code.
   * @returns ProgramDescr Program retrieved successfully.
   * @throws ApiError
   */
  public static getProgram(programId: string, withCode?: boolean | null): CancelablePromise<ProgramDescr> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/programs/{program_id}',
      path: {
        program_id: programId
      },
      query: {
        with_code: withCode
      },
      errors: {
        404: `Specified program id does not exist.`
      }
    })
  }

  /**
   * Delete a program.
   * Delete a program.
   *
   * Deletion fails if there is at least one pipeline associated with the program.
   * @param programId Unique program identifier
   * @returns any Program successfully deleted.
   * @throws ApiError
   */
  public static deleteProgram(programId: string): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'DELETE',
      url: '/programs/{program_id}',
      path: {
        program_id: programId
      },
      errors: {
        400: `Specified program id is referenced by a pipeline or is not a valid uuid.`,
        404: `Specified program id does not exist.`
      }
    })
  }

  /**
   * Change one or more of a program's code, description or name.
   * Change one or more of a program's code, description or name.
   *
   * If a program's code changes, any ongoing compilation gets cancelled,
   * the program status is reset to `None`, and the program version
   * is incremented by 1.
   *
   * Changing only the program's name or description does not affect its
   * version or the compilation process.
   * @param programId Unique program identifier
   * @param requestBody
   * @returns UpdateProgramResponse Program updated successfully.
   * @throws ApiError
   */
  public static updateProgram(
    programId: string,
    requestBody: UpdateProgramRequest
  ): CancelablePromise<UpdateProgramResponse> {
    return __request(OpenAPI, {
      method: 'PATCH',
      url: '/programs/{program_id}',
      path: {
        program_id: programId
      },
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `Specified program id does not exist.`,
        409: `A program with this name already exists in the database.`
      }
    })
  }

  /**
   * Mark a program for compilation.
   * Mark a program for compilation.
   *
   * The client can track a program's compilation status by pollling the
   * `/program/{program_id}` or `/programs` endpoints, and
   * then checking the `status` field of the program object
   * @param programId Unique program identifier
   * @param requestBody
   * @returns any Compilation request submitted.
   * @throws ApiError
   */
  public static compileProgram(programId: string, requestBody: CompileProgramRequest): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/programs/{program_id}/compile',
      path: {
        program_id: programId
      },
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `Specified program id does not exist.`,
        409: `Program version specified in the request doesn't match the latest program version in the database.`
      }
    })
  }
}
