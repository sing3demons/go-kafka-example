package middlewares

import (
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

func LoggingMiddleware() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		// Starting time request
		startTime := time.Now()
		// Processing request
		ctx.Next()
		// End Time request
		endTime := time.Now()
		// Request method
		reqMethod := ctx.Request.Method
		// Request route
		path := ctx.Request.RequestURI
		// status code
		statusCode := ctx.Writer.Status()
		// Request IP
		clientIP := ctx.ClientIP()
		// Request host
		host := ctx.Request.Host
		//
		userID, exists := ctx.Get("userId")
		if exists {
			userID = userID.(string)
		} else {
			userID = ""
		}

		reqId := ctx.Writer.Header().Get("X-Request-Id")
		if reqId == "" {
			reqId = "-"
		}

		body_size := ctx.Writer.Size()
		// execution time
		latencyTime := endTime.Sub(startTime)

		log.WithFields(log.Fields{
			"METHOD":        reqMethod,
			"STATUS":        statusCode,
			"LATENCY":       latencyTime,
			"CLIENT_IP":     clientIP,
			"REQUEST_ID":    reqId,
			"REMOTE_IP":     ctx.Request.RemoteAddr,
			"USER_ID":       userID,
			"USER_AGENT":    ctx.Request.UserAgent(),
			"ERROR":         ctx.Errors.ByType(gin.ErrorTypePrivate).String(),
			"REQUEST":       ctx.Request.PostForm.Encode(),
			"BODY_SIZE":     body_size,
			"HOST":          host,
			"PROTOCOL":      ctx.Request.Proto,
			"PATH":          path,
			"Query":         ctx.Request.URL.RawQuery,
			"ResponseSize":  ctx.Writer.Size(),
			"ContentType":   ctx.ContentType(),
			"ContentLength": ctx.Request.ContentLength,
			"TIMEZONE":      time.Now().Location().String(),
			"ISOTime":       startTime,
			"UnixTime":      startTime.UnixNano(),
			"USER":          ctx.Request.URL.User.Username(),
		}).Info("HTTP REQUEST")
		ctx.Next()
	}
}
