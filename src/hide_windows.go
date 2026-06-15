//go:build windows

package main

import "syscall"

func hidePath(path string) {
	ptr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return
	}
	syscall.SetFileAttributes(ptr, syscall.FILE_ATTRIBUTE_HIDDEN) //nolint:errcheck
}
