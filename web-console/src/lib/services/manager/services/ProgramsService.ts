/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { CompileProgramRequest } from '../models/CompileProgramRequest'
import type { CreateOrReplaceProgramRequest } from '../models/CreateOrReplaceProgramRequest'
import type { CreateOrReplaceProgramResponse } from '../models/CreateOrReplaceProgramResponse'
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
   * @returns ProgramDescr List of programs retrieved successfully
   * @throws ApiError
   */
  public static getPrograms(
    id?: string | null,
    name?: string | null,
    withCode?: boolean | null
  ): CancelablePromise<Array<ProgramDescr>> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/programs',
      query: {
        id: id,
        name: name,
        with_code: withCode
      },
      errors: {
        404: `Specified program name or ID does not exist`
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
      url: '/v0/programs',
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        409: `A program with this name already exists in the database`
      }
    })
  }
  /**
   * Fetch a program by name.
   * Fetch a program by name.
   * @param programName Unique program name
   * @param withCode Option to include the SQL program code or not
   * in the Program objects returned by the query.
   * If false (default), the returned program object
   * will not include the code.
   * @returns ProgramDescr Program retrieved successfully
   * @throws ApiError
   */
  public static getProgram(programName: string, withCode?: boolean | null): CancelablePromise<ProgramDescr> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/programs/{program_name}',
      path: {
        program_name: programName
      },
      query: {
        with_code: withCode
      },
      errors: {
        404: `Specified program name does not exist`
      }
    })
  }
  /**
   * Create or replace a program.
   * Create or replace a program.
   * @param programName Unique program name
   * @param requestBody
   * @returns CreateOrReplaceProgramResponse Program updated successfully
   * @throws ApiError
   */
  public static createOrReplaceProgram(
    programName: string,
    requestBody: CreateOrReplaceProgramRequest
  ): CancelablePromise<CreateOrReplaceProgramResponse> {
    return __request(OpenAPI, {
      method: 'PUT',
      url: '/v0/programs/{program_name}',
      path: {
        program_name: programName
      },
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        409: `A program with this name already exists in the database`
      }
    })
  }
  /**
   * Delete a program.
   * Delete a program.
   *
   * Deletion fails if there is at least one pipeline associated with the
   * program.
   * @param programName Unique program name
   * @returns any Program successfully deleted
   * @throws ApiError
   */
  public static deleteProgram(programName: string): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'DELETE',
      url: '/v0/programs/{program_name}',
      path: {
        program_name: programName
      },
      errors: {
        400: `Specified program is referenced by a pipeline`,
        404: `Specified program name does not exist`
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
   * @param programName Unique program name
   * @param requestBody
   * @returns UpdateProgramResponse Program updated successfully
   * @throws ApiError
   */
  public static updateProgram(
    programName: string,
    requestBody: UpdateProgramRequest
  ): CancelablePromise<UpdateProgramResponse> {
    return __request(OpenAPI, {
      method: 'PATCH',
      url: '/v0/programs/{program_name}',
      path: {
        program_name: programName
      },
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `Specified program name does not exist`,
        409: `A program with this name already exists in the database`
      }
    })
  }
  /**
   * Deprecated. Mark a program for compilation.
   * Deprecated. Mark a program for compilation.
   *
   * The client can track a program's compilation status by polling the
   * `/program/{program_name}` or `/programs` endpoints, and
   * then checking the `status` field of the program object.
   * @param programName Unique program name
   * @param requestBody
   * @returns any Compilation request submitted
   * @throws ApiError
   */
  public static compileProgram(programName: string, requestBody: CompileProgramRequest): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/v0/programs/{program_name}/compile',
      path: {
        program_name: programName
      },
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `Specified program name does not exist`,
        409: `Program version specified in the request doesn't match the latest program version in the database`
      }
    })
  }
}
