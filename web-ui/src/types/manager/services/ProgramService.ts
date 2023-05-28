/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { CancelProgramRequest } from '../models/CancelProgramRequest';
import type { CompileProgramRequest } from '../models/CompileProgramRequest';
import type { NewProgramRequest } from '../models/NewProgramRequest';
import type { NewProgramResponse } from '../models/NewProgramResponse';
import type { ProgramCodeResponse } from '../models/ProgramCodeResponse';
import type { ProgramDescr } from '../models/ProgramDescr';
import type { UpdateProgramRequest } from '../models/UpdateProgramRequest';
import type { UpdateProgramResponse } from '../models/UpdateProgramResponse';

import type { CancelablePromise } from '../core/CancelablePromise';
import { OpenAPI } from '../core/OpenAPI';
import { request as __request } from '../core/request';

export class ProgramService {

    /**
     * Returns program descriptor, including current program version and
     * Returns program descriptor, including current program version and
     * compilation status.
     * @param id Unique connector identifier
     * @param name Unique connector name
     * @returns ProgramDescr Program status retrieved successfully.
     * @throws ApiError
     */
    public static programStatus(
        id?: string,
        name?: string,
    ): CancelablePromise<ProgramDescr> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/v0/program',
            query: {
                'id': id,
                'name': name,
            },
            errors: {
                400: `Missing or invalid \`program_id\` parameter.`,
                404: `Specified \`program_id\` does not exist in the database.`,
            },
        });
    }

    /**
     * Returns the latest SQL source code of the program along with its meta-data.
     * Returns the latest SQL source code of the program along with its meta-data.
     * @param programId Unique program identifier
     * @returns ProgramCodeResponse Program data and code retrieved successfully.
     * @throws ApiError
     */
    public static programCode(
        programId: string,
    ): CancelablePromise<ProgramCodeResponse> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/v0/program/{program_id}/code',
            path: {
                'program_id': programId,
            },
            errors: {
                400: `Missing or invalid \`program_id\` parameter.`,
                404: `Specified \`program_id\` does not exist in the database.`,
            },
        });
    }

    /**
     * Enumerate the program database.
     * Enumerate the program database.
     * @returns ProgramDescr List of programs retrieved successfully
     * @throws ApiError
     */
    public static listPrograms(): CancelablePromise<Array<ProgramDescr>> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/v0/programs',
        });
    }

    /**
     * Create a new program.
     * Create a new program.
     *
     * If the `overwrite_existing` flag is set in the request and a program with
     * the same name already exists, all pipelines associated with that program and
     * the program itself will be deleted.
     * @param requestBody
     * @returns NewProgramResponse Program created successfully
     * @throws ApiError
     */
    public static newProgram(
        requestBody: NewProgramRequest,
    ): CancelablePromise<NewProgramResponse> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/v0/programs',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                409: `A program with this name already exists in the database.`,
            },
        });
    }

    /**
     * Change program code and/or name.
     * Change program code and/or name.
     *
     * If program code changes, any ongoing compilation gets cancelled,
     * program status is reset to `None`, and program version
     * is incremented by 1.  Changing program name only doesn't affect its
     * version or the compilation process.
     * @param requestBody
     * @returns UpdateProgramResponse Program updated successfully.
     * @throws ApiError
     */
    public static updateProgram(
        requestBody: UpdateProgramRequest,
    ): CancelablePromise<UpdateProgramResponse> {
        return __request(OpenAPI, {
            method: 'PATCH',
            url: '/v0/programs',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                404: `Specified \`program_id\` does not exist in the database.`,
                409: `A program with this name already exists in the database.`,
            },
        });
    }

    /**
     * Queue program for compilation.
     * Queue program for compilation.
     *
     * The client should poll the `/program_status` endpoint
     * for compilation results.
     * @param requestBody
     * @returns any Compilation request submitted.
     * @throws ApiError
     */
    public static compileProgram(
        requestBody: CompileProgramRequest,
    ): CancelablePromise<any> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/v0/programs/compile',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                404: `Specified \`program_id\` does not exist in the database.`,
                409: `Program version specified in the request doesn't match the latest program version in the database.`,
            },
        });
    }

    /**
     * Cancel outstanding compilation request.
     * Cancel outstanding compilation request.
     *
     * The client should poll the `/program_status` endpoint
     * to determine when the cancelation request completes.
     * @param requestBody
     * @returns any Cancelation request submitted.
     * @throws ApiError
     */
    public static cancelProgram(
        requestBody: CancelProgramRequest,
    ): CancelablePromise<any> {
        return __request(OpenAPI, {
            method: 'DELETE',
            url: '/v0/programs/compile',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                404: `Specified \`program_id\` does not exist in the database.`,
                409: `Program version specified in the request doesn't match the latest program version in the database.`,
            },
        });
    }

    /**
     * Delete a program.
     * Delete a program.
     *
     * Deletes all pipelines and configs associated with the program.
     * @param programId Unique program identifier
     * @returns any Program successfully deleted.
     * @throws ApiError
     */
    public static deleteProgram(
        programId: string,
    ): CancelablePromise<any> {
        return __request(OpenAPI, {
            method: 'DELETE',
            url: '/v0/programs/{program_id}',
            path: {
                'program_id': programId,
            },
            errors: {
                404: `Specified \`program_id\` does not exist in the database.`,
            },
        });
    }

}
