package command

import (
	"fmt"
	"log"
	"net/url"
	"os"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/urfave/cli/v2"
)

var initcql = `
CREATE CONSTRAINT unique_repository_id
ON (repository:Repository) ASSERT repository.id IS UNIQUE;

CREATE CONSTRAINT unique_author_id
ON (author:Author) ASSERT author.id IS UNIQUE;

CREATE CONSTRAINT unique_commit_hash
ON (commit:Commit) ASSERT commit.hash IS UNIQUE;
`

var GitSourceInputs struct {
	Neo4jURI    string
	Repository  string
	InitCQLPath string
}

var GitSourceCommand *cli.Command = &cli.Command{
	Name:        "source",
	Description: "Source an events from git repository",
	Usage:       "Source an events from git repository",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:        "repository",
			Destination: &GitSourceInputs.Repository,
			Aliases:     []string{"r"},
			Required:    true,
		},
		&cli.StringFlag{
			Name:        "neo4j-uri",
			Destination: &GitSourceInputs.Neo4jURI,
			Aliases:     []string{"u"},
			Value:       "bolt://localhost:7687",
		},
		&cli.StringFlag{
			Name:        "initcql-path",
			Destination: &GitSourceInputs.InitCQLPath,
			Aliases:     []string{"p"},
			Value:       "bolt://localhost:7687",
		},
	},
	Action: func(c *cli.Context) error {
		u, err := url.Parse(GitSourceInputs.Repository)
		if err != nil {
			return err
		}

		tmpdir := "data/" + u.Path
		if err := os.MkdirAll(tmpdir, 0755); err != nil {
			log.Println("ERR: ", err)
		}

		repo, err := git.PlainCloneContext(c.Context, tmpdir, false, &git.CloneOptions{
			URL:      GitSourceInputs.Repository,
			Progress: os.Stdout,
		})
		if err == git.ErrRepositoryAlreadyExists {
			repo, err = git.PlainOpen(tmpdir)
		}
		if err != nil {
			return err
		}

		iter, err := repo.Log(&git.LogOptions{All: true})
		if err != nil {
			return err
		}

		driver, err := neo4j.NewDriver(GitSourceInputs.Neo4jURI, neo4j.NoAuth(), func(c *neo4j.Config) {
			c.Encrypted = false
		})
		if err != nil {
			return err
		}
		defer driver.Close()

		session, err := driver.Session(neo4j.AccessModeWrite)
		if err != nil {
			return fmt.Errorf("failed to start session: %w", err)
		}
		defer session.Close()

		// init constraints
		_, err = session.Run(initcql, nil)
		if err != nil {
			log.Println("failed to init constraints: ", err)
		}

		_, err = session.Run(`CREATE (a:Repository) SET a.id = $id`, map[string]interface{}{"id": u.Path})
		if err != nil {
			return fmt.Errorf("failed to create Repository node: %w", err)
		}

		if err := iter.ForEach(func(commit *object.Commit) error {
			_, err = session.Run(`CREATE (a:Author) SET a.id = $id`, map[string]interface{}{"id": commit.Author.Email})
			if err != nil {
				return fmt.Errorf("failed to create Author node: %w", err)
			}

			_, err = session.Run(`
			MATCH (r:Repository), (a:Author) 
				WHERE 
					r.id = $repository_id 
					AND 
					a.id = $author_id
			CREATE (a)-[c:Commit { hash: $hash }]->(r)
			RETURN type(c), c.hash`,
				map[string]interface{}{
					"repository_id": u.Path,
					"author_id":     commit.Author.Email,
					"hash":          commit.Hash.String(),
				})
			if err != nil {
				return fmt.Errorf("failed to create Commit node: %w", err)
			}
			return nil
		}); err != nil {
			return err
		}

		return nil
	},
}
