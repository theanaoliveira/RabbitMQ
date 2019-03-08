FROM microsoft/dotnet:2.2-runtime-stretch-slim AS base
WORKDIR /app

FROM microsoft/dotnet:2.2-sdk-stretch AS build
WORKDIR /src
COPY ["RabbitMQ.Docker/RabbitMQ.Docker.csproj", "RabbitMQ.Docker/"]
RUN dotnet restore "RabbitMQ.Docker/RabbitMQ.Docker.csproj"
COPY . .
WORKDIR "/src/RabbitMQ.Docker"
RUN dotnet build "RabbitMQ.Docker.csproj" -c Release -o /app

FROM build AS publish
RUN dotnet publish "RabbitMQ.Docker.csproj" -c Release -o /app

FROM base AS final
WORKDIR /app
COPY --from=publish /app .
ENTRYPOINT ["dotnet", "RabbitMQ.Docker.dll"]
