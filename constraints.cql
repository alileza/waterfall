CREATE CONSTRAINT unique_repository_id
    ON (repository:Repository) ASSERT repository.id IS UNIQUE;

CREATE CONSTRAINT unique_author_id
    ON (author:Author) ASSERT author.id IS UNIQUE;
