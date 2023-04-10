/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { CancelProjectRequest } from '../models/CancelProjectRequest'
import type { CompileProjectRequest } from '../models/CompileProjectRequest'
import type { NewProjectRequest } from '../models/NewProjectRequest'
import type { NewProjectResponse } from '../models/NewProjectResponse'
import type { ProjectCodeResponse } from '../models/ProjectCodeResponse'
import type { ProjectDescr } from '../models/ProjectDescr'
import type { UpdateProjectRequest } from '../models/UpdateProjectRequest'
import type { UpdateProjectResponse } from '../models/UpdateProjectResponse'

import type { CancelablePromise } from '../core/CancelablePromise'
import { OpenAPI } from '../core/OpenAPI'
import { request as __request } from '../core/request'

export class ProjectService {
  /**
   * Enumerate the project database.
   * Enumerate the project database.
   * @returns ProjectDescr List of projects retrieved successfully
   * @throws ApiError
   */
  public static listProjects(): CancelablePromise<Array<ProjectDescr>> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/projects'
    })
  }

  /**
   * Create a new project.
   * Create a new project.
   *
   * If the `overwrite_existing` flag is set in the request and a project with
   * the same name already exists, all pipelines associated with that project and
   * the project itself will be deleted.
   * @param requestBody
   * @returns NewProjectResponse Project created successfully
   * @throws ApiError
   */
  public static newProject(requestBody: NewProjectRequest): CancelablePromise<NewProjectResponse> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/projects',
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        409: `A project with this name already exists in the database.`
      }
    })
  }

  /**
   * Change project code and/or name.
   * Change project code and/or name.
   *
   * If project code changes, any ongoing compilation gets cancelled,
   * project status is reset to `None`, and project version
   * is incremented by 1.  Changing project name only doesn't affect its
   * version or the compilation process.
   * @param requestBody
   * @returns UpdateProjectResponse Project updated successfully.
   * @throws ApiError
   */
  public static updateProject(requestBody: UpdateProjectRequest): CancelablePromise<UpdateProjectResponse> {
    return __request(OpenAPI, {
      method: 'PATCH',
      url: '/projects',
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `Specified \`project_id\` does not exist in the database.`,
        409: `A project with this name already exists in the database.`
      }
    })
  }

  /**
   * Queue project for compilation.
   * Queue project for compilation.
   *
   * The client should poll the `/project_status` endpoint
   * for compilation results.
   * @param requestBody
   * @returns any Compilation request submitted.
   * @throws ApiError
   */
  public static compileProject(requestBody: CompileProjectRequest): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/projects/compile',
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `Specified \`project_id\` does not exist in the database.`,
        409: `Project version specified in the request doesn't match the latest project version in the database.`
      }
    })
  }

  /**
   * Cancel outstanding compilation request.
   * Cancel outstanding compilation request.
   *
   * The client should poll the `/project_status` endpoint
   * to determine when the cancelation request completes.
   * @param requestBody
   * @returns any Cancelation request submitted.
   * @throws ApiError
   */
  public static cancelProject(requestBody: CancelProjectRequest): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'DELETE',
      url: '/projects/compile',
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `Specified \`project_id\` does not exist in the database.`,
        409: `Project version specified in the request doesn't match the latest project version in the database.`
      }
    })
  }

  /**
   * Returns project descriptor, including current project version and
   * Returns project descriptor, including current project version and
   * compilation status.
   * @param projectId Unique project identifier
   * @returns ProjectDescr Project status retrieved successfully.
   * @throws ApiError
   */
  public static projectStatus(projectId: number): CancelablePromise<ProjectDescr> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/projects/{project_id}',
      path: {
        project_id: projectId
      },
      errors: {
        400: `Missing or invalid \`project_id\` parameter.`,
        404: `Specified \`project_id\` does not exist in the database.`
      }
    })
  }

  /**
   * Delete a project.
   * Delete a project.
   *
   * Deletes all pipelines and configs associated with the project.
   * @param projectId Unique project identifier
   * @returns any Project successfully deleted.
   * @throws ApiError
   */
  public static deleteProject(projectId: number): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'DELETE',
      url: '/projects/{project_id}',
      path: {
        project_id: projectId
      },
      errors: {
        404: `Specified \`project_id\` does not exist in the database.`
      }
    })
  }

  /**
   * Returns the latest SQL source code of the project along with its meta-data.
   * Returns the latest SQL source code of the project along with its meta-data.
   * @param projectId Unique project identifier
   * @returns ProjectCodeResponse Project data and code retrieved successfully.
   * @throws ApiError
   */
  public static projectCode(projectId: number): CancelablePromise<ProjectCodeResponse> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/projects/{project_id}/code',
      path: {
        project_id: projectId
      },
      errors: {
        400: `Missing or invalid \`project_id\` parameter.`,
        404: `Specified \`project_id\` does not exist in the database.`
      }
    })
  }
}
