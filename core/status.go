package core

import (
	"time"

	"github.com/owncast/owncast/config"
	"github.com/owncast/owncast/core/data"
	"github.com/owncast/owncast/models"
	"github.com/roistat/go-clickhouse"
)

func DbInsert() {
	transport := clickhouse.NewHttpTransport()
	conn := clickhouse.NewConn("http://47.99.137.224:8123", transport)
	err := conn.Ping()
	if err != nil {
		panic(err)
	}

	userID := config.GlobalId
	currentTime := time.Now()
	ts := currentTime.Format("2006-01-02 15:04:05")
	gender := config.GlobalGender
	username := config.GlobalUsername
	currentViewer := config.GlobalViewer
	streamAddress := config.GlobalStreamAddress
	age := config.GlobalAge
	address := config.GlobalAddress
	q := clickhouse.NewQuery("INSERT INTO default.live_streaming (user_id, current_viewer, ts, gender, username, age, address, stream_address) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", userID, currentViewer, ts, gender, username, age, address, streamAddress)
	q.Exec(conn)
}

// GetStatus gets the status of the system.
func GetStatus() models.Status {
	if _stats == nil {
		return models.Status{}
	}

	viewerCount := 0
	if IsStreamConnected() {
		viewerCount = len(_stats.Viewers)
		config.GlobalViewer = viewerCount
	}

	return models.Status{
		Online:                IsStreamConnected(),
		ViewerCount:           viewerCount,
		OverallMaxViewerCount: _stats.OverallMaxViewerCount,
		SessionMaxViewerCount: _stats.SessionMaxViewerCount,
		LastDisconnectTime:    _stats.LastDisconnectTime,
		LastConnectTime:       _stats.LastConnectTime,
		VersionNumber:         config.VersionNumber,
		StreamTitle:           data.GetStreamTitle(),
	}
}

// GetCurrentBroadcast will return the currently active broadcast.
func GetCurrentBroadcast() *models.CurrentBroadcast {
	return _currentBroadcast
}

// setBroadcaster will store the current inbound broadcasting details.
func setBroadcaster(broadcaster models.Broadcaster) {
	_broadcaster = &broadcaster
}

// GetBroadcaster will return the details of the currently active broadcaster.
func GetBroadcaster() *models.Broadcaster {
	return _broadcaster
}
