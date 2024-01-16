package handlers

import (
	"github.com/graphql-go/graphql"
	"reconciler.io/models"
)

var queryType = graphql.NewObject(
	graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			// Define your queries here
			"getReconciliationTaskStatus": &graphql.Field{
				Type: models.ReconTaskDetails{},
				Args: graphql.FieldConfigArgument{
					"taskID": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Implement the logic to fetch task status
				},
			},
		},
	},
)

var mutationType = graphql.NewObject(
	graphql.ObjectConfig{
		Name: "Mutation",
		Fields: graphql.Fields{
			// Define your mutations here
			"createReconciliationTask": &graphql.Field{
				Type: reconTaskDetailsType,
				Args: graphql.FieldConfigArgument{
					// Define the arguments required for creating a task
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Implement the logic to create a task
				},
			},
		},
	},
)

var schema, _ = graphql.NewSchema(
	graphql.SchemaConfig{
		Query:    queryType,
		Mutation: mutationType,
	},
)
