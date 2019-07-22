param (
    [string]$version = $(throw "-version is required")
)

$ErrorActionPreference = "Stop"

Import-Module ./pawnee-deployment.psm1 -Force

Push-Package -projectName "Pawnee.Core" -version $version
Push-Package -projectName "Pawnee.AzureBindings" -version $version
Push-Package -projectName "Pawnee.FileSystemBindings" -version $version
