-- Input vertices
CREATE TABLE vertices (
    vertex BIGINT NOT NULL
);

-- Input edges
CREATE TABLE edges (
    src  BIGINT NOT NULL,
    dest BIGINT NOT NULL
);

CREATE VIEW unconnected_vertices_not_in AS
    SELECT vertex FROM vertices
    WHERE vertex NOT IN (
        SELECT src FROM edges
        UNION (SELECT dest FROM edges)
    );

CREATE VIEW unconnected_vertices_not_exists AS
    SELECT vertex FROM vertices
    WHERE NOT EXISTS (
        SELECT src FROM edges
        WHERE vertex = src OR vertex = dest
    );

CREATE VIEW unconnected_vertices_leftjoin AS
    SELECT vertex FROM vertices
    LEFT JOIN edges
      ON vertex = src OR vertex = dest
      WHERE src IS NULL AND dest IS NULL;
