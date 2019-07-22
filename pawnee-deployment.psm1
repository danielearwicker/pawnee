$ErrorActionPreference = "Stop"

function Push-Package($projectName, $version) {
	pushd $(Join-Path $PSScriptRoot $projectName)
	pwd
	dotnet pack -c release -p:PackageVersion=$version
	dotnet nuget push bin\release\$projectName.$version.nupkg -k $Env:SECRET_NUGET_APIKEY -s https://api.nuget.org/v3/index.json
	popd
}

function Push-Image($imageName, $projectPath) {
	pushd $projectPath
	dotnet publish -c release -o published 	
	docker build . -t $imageName	
	docker push $imageName
	popd	
}

function Push-CoreImages($version) {

	$imageNames = @{
		watchdog = "danielearwicker/pawnee-watchdog:$version";
		dashboard = "danielearwicker/pawnee-dashboard:$version"
	}
	
	Push-Image -imageName $imageNames.watchdog -projectPath $(Join-Path $PSScriptRoot "Pawnee.Watchdog")
	Push-Image -imageName $imageNames.dashboard -projectPath $(Join-Path $PSScriptRoot "Pawnee.Dashboard")

	$imageNames
}

function Deploy-AzureResources($location, $name) {

	$config = @{
		location = $location;
		groupName = $name;
		azStorageName = $name;
		redisName = $name;
		identityName = $name;
	}
	
	az group create -l $location -n $config.groupName
	az redis create -l $location -n $config.redisName -g $config.groupName --sku Basic --vm-size c0
	az storage account create -n $config.azStorageName -g $config.groupName -l $location --sku Standard_LRS
	az identity create -n $config.identityName -g $config.groupName

	$config.azStorageCs = $(az storage account show-connection-string -g $config.groupName -n $config.azStorageName --query connectionString -o tsv)
	$config.redisKey = $(az redis list-keys -n $config.redisName -g $config.groupName --query primaryKey -o tsv)
	$config.redisCs = "$($config.redisName).redis.cache.windows.net:6380,password=$($config.redisKey),ssl=True,abortConnect=False"
	$config.subscriptionID = $(az account show --query id -o tsv)
	$config.tenantID = $(az account show --query tenantId -o tsv)
	$config.identityResource = $(az identity show -g $config.groupName -n $config.identityName --query id --output tsv)

	$config
}

function Deploy-Containers($config, $coreImageNames, $workerImageName) {

	az container create -g $config.groupName -n pawnee-watchdog `
             --image $coreImageNames.watchdog `
             --assign-identity $config.identityResource `
             -l $config.location --cpu 1 --memory 1 `
             -e "PAWNEE_AZSTORAGE=$($config.azStorageCs)" `
                "PAWNEE_REDIS=$($config.redisCs)" `
                "PAWNEE_RESOURCEGROUP=$($config.groupName)" `
                "PAWNEE_SUBSCRIPTION=$($config.subscriptionID)" `
                "PAWNEE_TENANTID=$($config.tenantID)" `
                "PAWNEE_WORKERIMAGE=$workerImageName"

	az container create -g $config.groupName -n pawnee-dashboard `
             --image $coreImageNames.dashboard `
			 --ports 80 --dns-name-label pawnee-dashboard `
             -l $config.location --cpu 1 --memory 1 `
             -e "PAWNEE_AZSTORAGE=$($config.azStorageCs)" `
                "PAWNEE_REDIS=$($config.redisCs)"
}

function Save-DevEnvVars($config) {
	[Environment]::SetEnvironmentVariable("PAWNEE_AZSTORAGE", $config.azStorageCs, "User")
	[Environment]::SetEnvironmentVariable("PAWNEE_REDIS", $config.redisCs, "User")
	[Environment]::SetEnvironmentVariable("PAWNEE_RESOURCEGROUP", $config.groupName, "User")
	[Environment]::SetEnvironmentVariable("PAWNEE_TENANTID", $config.tenantID, "User")
	[Environment]::SetEnvironmentVariable("PAWNEE_SUBSCRIPTION", $config.subID, "User")
	[Environment]::SetEnvironmentVariable("PAWNEE_WORKERIMAGE", $config.workerImageName, "User")
}
