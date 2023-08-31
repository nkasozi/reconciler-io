package http

import (
	"github.com/gin-gonic/gin"
	"go.temporal.io/sdk/client"
)

type RestApiServer struct {
	*gin.Engine
	TemporalClient client.Client
}

func NewRestApiServer() *RestApiServer {
	r := gin.Default()
	return &RestApiServer{
		Engine: r,
	}
}
