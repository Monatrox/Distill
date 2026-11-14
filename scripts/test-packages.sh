#!/usr/bin/env bash
# Restore the produced packages into a fresh consumer without project references.
set -euo pipefail
feed="$(realpath "${1:-artifacts}")"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT
python3 - "$work_dir" <<'PY'
from pathlib import Path
import os, sys
root = Path(sys.argv[1])
fallback = root / 'fallback'
fallback.mkdir()
cache = Path(os.environ.get('NUGET_PACKAGES', Path.home() / '.nuget/packages'))
if cache.exists():
    for package in cache.iterdir():
        if package.is_dir() and not package.name.startswith('distill.'):
            (fallback / package.name).symlink_to(package, target_is_directory=True)
PY
cat > "$work_dir/Consumer.csproj" <<'XML'
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
    <EnableDefaultCompileItems>false</EnableDefaultCompileItems>
  </PropertyGroup>
  <ItemGroup>
    <Compile Include="Program.cs" />
    <PackageReference Include="Distill.Client" Version="0.1.0" />
    <PackageReference Include="Distill.Extensions.Logging" Version="0.1.0" />
    <PackageReference Include="Distill.Aspire" Version="0.1.0" />
  </ItemGroup>
</Project>
XML
cat > "$work_dir/Program.cs" <<'CS'
using Distill.Client;
using Distill.Extensions.Logging;
using Distill.Aspire;
using var http = new HttpClient { BaseAddress = new Uri("https://unused.test") };
using var client = new DistillClient(http, "package-contract-key");
await using var provider = new DistillLoggerProvider(client);
var request = new CreateChannelRequest { Name = "package.test", Version = "1", SinkId = "receiver@1" };
Console.WriteLine($"Loaded {typeof(DistillResource).Name}, logger and version {request.Version} from packages.");
CS
dotnet restore "$work_dir/Consumer.csproj" --source "$feed" --source https://api.nuget.org/v3/index.json \
    -p:RestorePackagesPath="$work_dir/packages" -p:RestoreFallbackFolders="$work_dir/fallback" -m:1 -nr:false
dotnet run --project "$work_dir/Consumer.csproj" --no-restore -p:BuildInParallel=false -m:1 -nr:false
