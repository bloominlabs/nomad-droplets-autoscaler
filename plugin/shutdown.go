package plugin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/digitalocean/godo"
	"github.com/hashicorp/go-hclog"
)

func shutdownDroplet(
	dropletId int,
	client *godo.Client,
	log hclog.Logger) error {

	// Gracefully power off the droplet.
	log.Debug("Gracefully shutting down droplet...")
	_, _, err := client.DropletActions.PowerOff(context.TODO(), dropletId)
	if err != nil {
		// If we get an error the first time, actually report it
		return fmt.Errorf("error shutting down droplet: %s", err)
	}

	err = waitForDropletState("off", dropletId, client, log, 5*time.Minute)
	if err != nil {
		log.Warn("Timeout while waiting to for droplet to become 'off'")
	}

	log.Debug("Deleting Droplet...")
	_, err = client.Droplets.Delete(context.TODO(), dropletId)
	if err != nil {
		var doError *godo.ErrorResponse
		if errors.As(err, &doError) && doError.Response.StatusCode == http.StatusNotFound {
			log.Warn("encountered 404 while deleting droplet. assuming it was already deleted an continuing")
		} else {
			return fmt.Errorf("error deleting droplet: %s", err)
		}
	}

	return nil
}
