package test

import (
	"corekv/utils"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManifest(t *testing.T) {
	clearDir()
	recovery := func() {
		lsm := buildLSM()
		baseTest(t, lsm, 128)
		lsm.Close()
	}
	runTest(5, recovery)
}

func helpTestManifestFileCorruption(t *testing.T, off int64, errorContent string) {
	clearDir()
	{
		lsm := buildLSM()
		require.NoError(t, lsm.Close())
	}
	fp, err := os.OpenFile(filepath.Join(opt.WorkDir, utils.ManifestFilename), os.O_RDWR, 0)
	require.NoError(t, err)

	_, err = fp.WriteAt([]byte{'X'}, off)
	require.NoError(t, err)
	require.NoError(t, fp.Close())
	defer func() {
		if err := recover(); err != nil {
			require.Contains(t, err.(error).Error(), errorContent)
		}
	}()
	lsm := buildLSM()
	require.NoError(t, lsm.Close())
}
