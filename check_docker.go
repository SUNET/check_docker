package main

import (
	"encoding/json"
	"errors"
	"flag"
        "strconv"
	"fmt"
	dockerlib "github.com/fsouza/go-dockerclient"
	"github.com/SUNET/go_nagios"
	"github.com/shenwei356/util/bytesize"
	"strings"
	"sync"
	"time"
)

func NewCheckDocker(endpoint string) (*CheckDocker, error) {
	var err error

	cd := &CheckDocker{}
	cd.WarnMetaSpace = 100 // defaults
	cd.CritMetaSpace = 100
	cd.WarnDataSpace = 100
	cd.CritDataSpace = 100
	cd.CheckRestartCount = false
	cd.WarnRestartCount = 1
	cd.CritRestartCount = 2
	cd.CheckUptime = false
	cd.WarnUptimeSecs = 3600
	cd.CritUptimeSecs = 1200

	if endpoint != "" {
		err = cd.setupClient(endpoint)
	}

	return cd, err
}

type CheckDocker struct {
	WarnMetaSpace        float64
	CritMetaSpace        float64
	WarnDataSpace        float64
	CritDataSpace        float64
	CheckRestartCount    bool
	WarnRestartCount     int
	CritRestartCount     int
	CheckUptime          bool
	WarnUptimeSecs       int64
	CritUptimeSecs       int64
	ImageId              string
	ContainerName        string
	TLSCertPath          string
	TLSKeyPath           string
	TLSCAPath            string
	dockerclient         *dockerlib.Client
	dockerInfoData       *dockerlib.Env
	dockerContainersData []dockerlib.APIContainers
}

func (cd *CheckDocker) setupClient(endpoint string) error {
	var err error

	if cd.TLSCertPath != "" && cd.TLSKeyPath != "" && cd.TLSCAPath != "" {
		cd.dockerclient, err = dockerlib.NewTLSClient(endpoint, cd.TLSCertPath, cd.TLSKeyPath, cd.TLSCAPath)
	} else {
		cd.dockerclient, err = dockerlib.NewClient(endpoint)
	}

	return err
}

func (cd *CheckDocker) GetData() error {
	errChan := make(chan error)
	var err error
	var wg sync.WaitGroup

	wg.Add(2)

	go func(cd *CheckDocker, errChan chan error) {
		defer wg.Done()

		cd.dockerInfoData, err = cd.dockerclient.Info()
		if err != nil {
			errChan <- err
		}
	}(cd, errChan)

	go func(cd *CheckDocker, errChan chan error) {
		defer wg.Done()

		cd.dockerContainersData, err = cd.dockerclient.ListContainers(dockerlib.ListContainersOptions{})
		if err != nil {
			errChan <- err
		}
	}(cd, errChan)

	go func() {
		wg.Wait()
		close(errChan)
	}()

	err = <-errChan

	return err
}

func (cd *CheckDocker) getByteSizeDriverStatus(key string) (bytesize.ByteSize, error) {
	var statusInArray [][]string

	err := json.Unmarshal([]byte(cd.dockerInfoData.Get("DriverStatus")), &statusInArray)

	if err != nil {
		return -1, errors.New("Unable to extract DriverStatus info.")
	}

	for _, status := range statusInArray {
		if status[0] == key {
			return bytesize.Parse([]byte(status[1]))
		}
	}

	return -1, errors.New(fmt.Sprintf("DriverStatus does not contain \"%v\"", key))
}

func (cd *CheckDocker) GetDataSpaceUsed() (bytesize.ByteSize, error) {
	return cd.getByteSizeDriverStatus("Data Space Used")
}

func (cd *CheckDocker) GetDataSpaceTotal() (bytesize.ByteSize, error) {
	return cd.getByteSizeDriverStatus("Data Space Total")
}

func (cd *CheckDocker) GetMetaSpaceUsed() (bytesize.ByteSize, error) {
	return cd.getByteSizeDriverStatus("Metadata Space Used")
}

func (cd *CheckDocker) GetMetaSpaceTotal() (bytesize.ByteSize, error) {
	return cd.getByteSizeDriverStatus("Metadata Space Total")
}

func (cd *CheckDocker) IsContainerRunning(imageId string) (dockerlib.APIContainers, bool) {
	for _, container := range cd.dockerContainersData {
		if strings.HasPrefix(container.Image, imageId) && strings.HasPrefix(container.Status, "Up") {
			return container, true
		}
	}
	return dockerlib.APIContainers{}, false
}

func (cd *CheckDocker) IsNamedContainerRunning(containerName string) (dockerlib.APIContainers, bool) {
	for _, container := range cd.dockerContainersData {
		for _, name := range container.Names {
			// Container names start with a slash for some reason, maybe a bug?
			// Remove the leading / only if it's found so this doesn't break in
			// the future.
			if strings.HasPrefix(name, "/") {
				name = name[1:]
			}
			if name == containerName {
				if strings.HasPrefix(container.Status, "Up") {
					return container, true
				} else {
					return dockerlib.APIContainers{}, false
				}
			}
		}
	}
	return dockerlib.APIContainers{}, false
}

func (cd* CheckDocker) GetContainerDetailsImageId(imageId string) (*dockerlib.Container, bool) {
	for _, container := range cd.dockerContainersData {
	  if container.ID == imageId {
			containerInfo, err := cd.dockerclient.InspectContainer(imageId)
			if err != nil {
			  return nil, false
			} else {
			  return containerInfo, true
			}
		}
	}
	return nil, false
}

func (cd* CheckDocker) GetContainerDetailsNamed(containerName string) (*dockerlib.Container, bool) {
	for _, container := range cd.dockerContainersData {
	  for _, name := range container.Names {
			// Container names start with a slash for some reason, maybe a bug?
			// Remove the leading / only if it's found so this doesn't break in
			// the future.
			if strings.HasPrefix(name, "/") {
				name = name[1:]
			}
			if name == containerName {
				containerInfo, err := cd.dockerclient.InspectContainer(containerName)
				if err != nil {
				  return nil, false
				} else {
				  return containerInfo, true
				}
			}
		}
	}
	return nil, false
}

func (cd *CheckDocker) IsContainerAGhost(imageId string) (dockerlib.APIContainers, bool) {
	for _, container := range cd.dockerContainersData {
		if strings.HasPrefix(container.Image, imageId) && strings.Contains(container.Status, "Ghost") {
			return container, true
		}
	}
	return dockerlib.APIContainers{}, false
}

func (cd *CheckDocker) CheckMetaSpace(warnThreshold, criticalThreshold float64) *nagios.NagiosStatus {
	usedByteSize, err := cd.GetMetaSpaceUsed()
	if err != nil {
		return &nagios.NagiosStatus{err.Error(), nagios.NAGIOS_CRITICAL}
	}

	totalByteSize, err := cd.GetMetaSpaceTotal()
	if err != nil {
		return &nagios.NagiosStatus{err.Error(), nagios.NAGIOS_CRITICAL}
	}

	percentUsed := float64(usedByteSize/totalByteSize) * 100

	status := &nagios.NagiosStatus{fmt.Sprintf("Metadata Space Usage: %f", percentUsed) + "%", nagios.NAGIOS_OK}

	if percentUsed >= warnThreshold {
		status.Value = nagios.NAGIOS_WARNING
	}
	if percentUsed >= criticalThreshold {
		status.Value = nagios.NAGIOS_CRITICAL
	}

	return status
}

func (cd *CheckDocker) CheckDataSpace(warnThreshold, criticalThreshold float64) *nagios.NagiosStatus {
	usedByteSize, err := cd.GetDataSpaceUsed()
	if err != nil {
		return &nagios.NagiosStatus{err.Error(), nagios.NAGIOS_CRITICAL}
	}

	totalByteSize, err := cd.GetDataSpaceTotal()
	if err != nil {
		return &nagios.NagiosStatus{err.Error(), nagios.NAGIOS_CRITICAL}
	}

	percentUsed := float64(usedByteSize/totalByteSize) * 100

	status := &nagios.NagiosStatus{fmt.Sprintf("Data Space Usage: %f", percentUsed) + "%", nagios.NAGIOS_OK}

	if percentUsed >= warnThreshold {
		status.Value = nagios.NAGIOS_WARNING
	}
	if percentUsed >= criticalThreshold {
		status.Value = nagios.NAGIOS_CRITICAL
	}

	return status
}

func (cd *CheckDocker) CheckImageContainerStatus(imageId string) *nagios.NagiosStatus {
	container, isRunning := cd.IsContainerRunning(imageId)
	containerGhost, isGhost := cd.IsContainerAGhost(imageId)

	if !isRunning {
		return &nagios.NagiosStatus{fmt.Sprintf("Container(ID: %v) is not running.", imageId), nagios.NAGIOS_CRITICAL}
	}
	if isGhost {
		return &nagios.NagiosStatus{fmt.Sprintf("Container(ID: %v) of image: %v is in ghost state.", containerGhost.ID, imageId), nagios.NAGIOS_CRITICAL}
	}
	if isRunning && !isGhost {
    return &nagios.NagiosStatus{fmt.Sprintf("Container(ID: %v) of image: %v is working as expected.", container.ID, imageId), nagios.NAGIOS_OK}
	}
	return &nagios.NagiosStatus{fmt.Sprintf("Container of image: %v - status unknown.", imageId), nagios.NAGIOS_CRITICAL}
}

func (cd *CheckDocker) CheckNamedContainerStatus(containerName string) *nagios.NagiosStatus {
	container, isRunning := cd.IsNamedContainerRunning(containerName)
	_, isGhost := cd.IsContainerAGhost(container.ID)

	if !isRunning {
		return &nagios.NagiosStatus{fmt.Sprintf("Container named: %v is not running.", containerName), nagios.NAGIOS_CRITICAL}
	}
	if isGhost {
		return &nagios.NagiosStatus{fmt.Sprintf("Container(ID: %v) named: %v is in ghost state.", container.ID, containerName), nagios.NAGIOS_CRITICAL}
	}
	if isRunning && !isGhost {
		return &nagios.NagiosStatus{fmt.Sprintf("Container(ID: %v) named: %v is working as expected.", container.ID, containerName), nagios.NAGIOS_OK}
	}
	return &nagios.NagiosStatus{fmt.Sprintf("Container named: %v - status unknown.", containerName), nagios.NAGIOS_CRITICAL}
}


func (cd *CheckDocker) CheckImageContainerRestartCount(imageId string) *nagios.NagiosStatus {
	container, isRunning := cd.IsContainerRunning(imageId)
	containerGhost, isGhost := cd.IsContainerAGhost(imageId)

	if isRunning && !isGhost {
		containerDetails, success := cd.GetContainerDetailsImageId(imageId)
		if success {
			if containerDetails.RestartCount > cd.CritRestartCount {
			  return &nagios.NagiosStatus{fmt.Sprintf("Container(ID: %v) has been restarted %d times.", container.ID, containerDetails.RestartCount), nagios.NAGIOS_CRITICAL}
			} else if containerDetails.RestartCount > cd.WarnRestartCount {
			  return &nagios.NagiosStatus{fmt.Sprintf("Container(ID: %v) has been restarted %d times.", containerGhost.ID, containerDetails.RestartCount), nagios.NAGIOS_WARNING}
			} else {
        return &nagios.NagiosStatus{fmt.Sprintf("Container(ID: %v) of image: %v has an acceptable RestartCount of %d", container.ID, imageId, containerDetails.RestartCount), nagios.NAGIOS_OK}
			}
		}
	}
	return &nagios.NagiosStatus{fmt.Sprintf("Container of image: %v - status unknown.", imageId), nagios.NAGIOS_CRITICAL}
}

func (cd *CheckDocker) CheckNamedContainerRestartCount(containerName string) *nagios.NagiosStatus {
	container, isRunning := cd.IsNamedContainerRunning(containerName)
	_, isGhost := cd.IsContainerAGhost(container.ID)

	if isRunning && !isGhost {
		containerDetails, success := cd.GetContainerDetailsNamed(containerName)
		if success {
			if containerDetails.RestartCount > cd.CritRestartCount {
			  return &nagios.NagiosStatus{fmt.Sprintf("Container(ID: %v) named: %v has been restarted %d times.", container.ID, containerName, containerDetails.RestartCount), nagios.NAGIOS_CRITICAL}
			} else if containerDetails.RestartCount > cd.WarnRestartCount {
			  return &nagios.NagiosStatus{fmt.Sprintf("Container(ID: %v) named: %v has been restarted %d times.", container.ID, containerName, containerDetails.RestartCount), nagios.NAGIOS_WARNING}
			} else {
				return &nagios.NagiosStatus{fmt.Sprintf("Container(ID: %v) named: %v has an acceptable RestartCount of %d.", container.ID, containerName, containerDetails.RestartCount), nagios.NAGIOS_OK}
			}
		}
	}
	return &nagios.NagiosStatus{fmt.Sprintf("Container named: %v - status unknown.", containerName), nagios.NAGIOS_CRITICAL}
}

func (cd *CheckDocker) CheckImageContainerUptime(imageId string) *nagios.NagiosStatus {
	container, isRunning := cd.IsContainerRunning(imageId)
	containerGhost, isGhost := cd.IsContainerAGhost(imageId)

	if isRunning && !isGhost {
		containerDetails, success := cd.GetContainerDetailsImageId(imageId)
		if success {
			var now = time.Now().Unix()
			var started = containerDetails.State.StartedAt.Unix()
			var duration = (now - started)
			if duration < cd.CritUptimeSecs {
			  return &nagios.NagiosStatus{fmt.Sprintf("Container(ID: %v) has an uptime of %d (<%d critical threshold).", container.ID, duration, cd.CritUptimeSecs), nagios.NAGIOS_CRITICAL}
			} else if duration < cd.WarnUptimeSecs {
			  return &nagios.NagiosStatus{fmt.Sprintf("Container(ID: %v) has an uptime of %d (<%d warning threshold).", containerGhost.ID, duration, cd.WarnUptimeSecs), nagios.NAGIOS_WARNING}
			} else {
        return &nagios.NagiosStatus{fmt.Sprintf("Container(ID: %v) of image: %v has been up since %s.", container.ID, imageId, containerDetails.State.StartedAt), nagios.NAGIOS_OK}
			}
		}
	}
	return &nagios.NagiosStatus{fmt.Sprintf("Container of image: %v - status unknown.", imageId), nagios.NAGIOS_CRITICAL}
}

func (cd *CheckDocker) CheckNamedContainerUptime(containerName string) *nagios.NagiosStatus {
	container, isRunning := cd.IsNamedContainerRunning(containerName)
	_, isGhost := cd.IsContainerAGhost(container.ID)

	if isRunning && !isGhost {
		containerDetails, success := cd.GetContainerDetailsNamed(containerName)
		if success {
			var now = time.Now().Unix()
			var started = containerDetails.State.StartedAt.Unix()
			var duration = (now - started)
			if duration < cd.CritUptimeSecs {
			  return &nagios.NagiosStatus{fmt.Sprintf("Container(ID: %v) named: %v has an uptime of %d (<%d critical threshold).", container.ID, containerName, duration, cd.CritUptimeSecs), nagios.NAGIOS_CRITICAL}
			} else if duration < cd.WarnUptimeSecs {
			  return &nagios.NagiosStatus{fmt.Sprintf("Container(ID: %v) named: %v has an uptime of %d (<%d warning threshold).", container.ID, containerName, duration, cd.WarnUptimeSecs), nagios.NAGIOS_WARNING}
			} else {
				return &nagios.NagiosStatus{fmt.Sprintf("Container(ID: %v) named: %v has been up since %s.", container.ID, containerName, containerDetails.State.StartedAt), nagios.NAGIOS_OK}
			}
		}
	}
	return &nagios.NagiosStatus{fmt.Sprintf("Container named: %v - status unknown.", containerName), nagios.NAGIOS_CRITICAL}
}

func main() {
	cd, err := NewCheckDocker("")
	if err != nil {
		nagios.Critical(err)
	}

	var dockerEndpoint string

	flag.StringVar(&dockerEndpoint, "base-url", "http://localhost:2375", "The Base URL for the Docker server")
	flag.Float64Var(&cd.WarnMetaSpace, "warn-meta-space", 100, "Warning threshold for Metadata Space")
	flag.Float64Var(&cd.CritMetaSpace, "crit-meta-space", 100, "Critical threshold for Metadata Space")
	flag.Float64Var(&cd.WarnDataSpace, "warn-data-space", 100, "Warning threshold for Data Space")
	flag.Float64Var(&cd.CritDataSpace, "crit-data-space", 100, "Critical threshold for Data Space")
	flag.BoolVar(&cd.CheckRestartCount, "check-restart", false, "Check the number of automated container restarts for the specified container")
	flag.IntVar(&cd.WarnRestartCount, "warn-restart-count", 1, "Warning threshold for automated restarts")
	flag.IntVar(&cd.CritRestartCount, "crit-restart-count", 2, "Critical threshold for automated restarts")
	flag.BoolVar(&cd.CheckUptime, "check-uptime", false, "Check the expected uptime of the specified container")
	flag.Int64Var(&cd.WarnUptimeSecs, "warn-uptime-secs", 3600, "Warning threshold for uptime")
	flag.Int64Var(&cd.CritUptimeSecs, "crit-uptime-secs", 1200, "Critical threshold for uptime")
	flag.StringVar(&cd.ImageId, "image-id", "", "An image ID that must be running on the Docker server")
	flag.StringVar(&cd.ContainerName, "container-name", "", "The name of a container that must be running on the Docker server")
	flag.StringVar(&cd.TLSCertPath, "tls-cert", "", "Path to TLS cert file.")
	flag.StringVar(&cd.TLSKeyPath, "tls-key", "", "Path to TLS key file.")
	flag.StringVar(&cd.TLSCAPath, "tls-ca", "", "Path to TLS CA file.")

	flag.Parse()

	err = cd.setupClient(dockerEndpoint)
	if err != nil {
		nagios.Critical(err)
	}

	err = cd.GetData()
	if err != nil {
		nagios.Critical(err)
	}

	baseStatus := &nagios.NagiosStatus{fmt.Sprintf("Total Containers: %v", len(cd.dockerContainersData)), nagios.NAGIOS_OK}

	statuses := make([]*nagios.NagiosStatus, 0)

	driver := cd.dockerInfoData.Get("Driver")

	// Unfortunately, Metadata Space and Data Space information is only available on devicemapper
	if driver == "devicemapper" {
		statuses = append(statuses, cd.CheckMetaSpace(cd.WarnMetaSpace, cd.CritMetaSpace))
		statuses = append(statuses, cd.CheckDataSpace(cd.WarnDataSpace, cd.CritDataSpace))
	}

	if cd.ImageId != "" {
		statuses = append(statuses, cd.CheckImageContainerStatus(cd.ImageId))
		if cd.CheckRestartCount {
		statuses = append(statuses, cd.CheckImageContainerRestartCount(cd.ImageId))
		}
		if cd.CheckUptime {
			statuses = append(statuses, cd.CheckImageContainerUptime(cd.ImageId))
		}
	}

	if cd.ContainerName != "" {
		statuses = append(statuses, cd.CheckNamedContainerStatus(cd.ContainerName))
		if cd.CheckRestartCount {
		statuses = append(statuses, cd.CheckNamedContainerRestartCount(cd.ContainerName))
		}
		if cd.CheckUptime {
			statuses = append(statuses, cd.CheckNamedContainerUptime(cd.ContainerName))
		}
	}

	perfdata := nagios.NagiosPerformanceVal{"containers", strconv.Itoa(len(cd.dockerContainersData)), "", "", "", "", ""}
	perfStatus := nagios.NagiosStatusWithPerformanceData{baseStatus, perfdata}

	perfStatus.Aggregate(statuses)
	perfStatus.NagiosExit()
}
