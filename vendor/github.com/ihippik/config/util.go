package config

import "runtime/debug"

// GetVersion returns latest git hash of commit.
func GetVersion() string {
	var version = "unknown"

	info, ok := debug.ReadBuildInfo()
	if ok {
		for _, item := range info.Settings {
			if item.Key == "vcs.revision" && len(item.Value) > 4 {
				version = item.Value[:4]
			}
		}
	}

	return version
}
