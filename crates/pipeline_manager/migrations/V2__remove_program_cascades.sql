-- We do not want pipelines to be deleted when programs they reference are deleted.
-- So remove DELETE CASCADE from pipeline -> program foreign key
ALTER TABLE pipeline
DROP CONSTRAINT pipeline_program_id_tenant_id_fkey,
ADD CONSTRAINT pipeline_program_id_tenant_id_fkey FOREIGN KEY (program_id, tenant_id) REFERENCES program(id, tenant_id);