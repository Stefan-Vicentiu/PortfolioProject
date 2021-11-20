SELECT *
FROM Projects..CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 3,4

--SELECT *
--FROM Projects..CovidVaccinations
--ORDER BY 3,4


--Select Data that we are going to be using

SELECT Location, date, total_cases, new_cases, total_deaths, population
FROM Projects..CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 1,2


-- Looking at Total Cases vs Total Deaths
-- Shows likelihood of dying if you contract covid in your country
SELECT Location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS DeathPercentage
FROM Projects..CovidDeaths
WHERE location LIKE '%Romania%' AND continent IS NOT NULL
ORDER BY 1,2


-- Looking at Total Cases vs Population
-- Shows what percentage of population got Covid
SELECT Location, date, total_cases, population, (total_cases/population)*100 AS CasesPercentage
FROM Projects..CovidDeaths
WHERE location LIKE '%Romania%' AND continent IS NOT NULL
ORDER BY 1,2


-- Looking at Countries with Highest Infection Rate compared to Population

SELECT Location, population, MAX(total_cases) AS HighestInfectionCount,  MAX((total_cases/population))*100 AS PercentPopulationInfected
FROM Projects..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY Location, population
ORDER BY PercentPopulationInfected DESC
--ORDER BY PercentPopulationInfected ASC


-- Showing Countries with Highest Death Count per Population

SELECT Location, MAX(cast(total_deaths AS INT)) AS TotalDeathCount
FROM Projects..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY Location
ORDER BY TotalDeathCount DESC


-- Break down by Continent with location

SELECT Location, MAX(cast(total_deaths AS INT)) AS TotalDeathCount
FROM Projects..CovidDeaths
WHERE continent IS NULL
GROUP BY Location
ORDER BY TotalDeathCount DESC


-- Break down by Continent with highest death count per population

SELECT continent, MAX(cast(total_deaths AS INT)) AS TotalDeathCount
FROM Projects..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY TotalDeathCount DESC


-- Global numbers by date

SELECT date, SUM(new_cases) AS total_cases, SUM(CAST(new_deaths AS INT)) AS total_deaths, SUM(CAST(new_deaths AS INT))/SUM(new_cases)*100 AS DeathPercentage
FROM Projects..CovidDeaths
-- WHERE location LIKE '%Romania%' AND 
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY 1,2


-- Global numbers

SELECT SUM(new_cases) AS total_cases, SUM(CAST(new_deaths AS INT)) AS total_deaths, SUM(CAST(new_deaths AS INT))/SUM(new_cases)*100 AS DeathPercentage
FROM Projects..CovidDeaths
-- WHERE location LIKE '%Romania%' AND 
WHERE continent IS NOT NULL
--GROUP BY date
ORDER BY 1,2


-- Global numbers death vs population

SELECT SUM(population) AS GlobalPopulation, SUM(new_cases) AS total_new_cases, SUM(CAST(new_deaths AS INT)) AS total_new_deaths, SUM(new_cases)/SUM(population)*100 AS GlobalInfectedPopulation, SUM(CAST(new_deaths AS INT))/SUM(population)*100 AS GlobalDeathPopulation , SUM(CAST(new_deaths AS INT))/SUM(new_cases)*100 AS InfestationDeathPercentage 
FROM Projects..CovidDeaths
-- WHERE location LIKE '%Romania%' AND 
WHERE continent IS NOT NULL
--GROUP BY date
ORDER BY 1,2


-- Looking at Total Population vs Vaccinations

SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(CONVERT(bigint, vac.new_vaccinations)) OVER (Partition by dea.location ORDER BY dea.location, dea.date) AS SumOfPeopleVaccination
FROM Projects..CovidDeaths dea
JOIN Projects..CovidVaccinations vac
	ON dea.location = vac.location AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
ORDER BY 2,3


-- USE CTE

WITH PopVsVac (Continet, Location, Date, Population, New_Vaccinations, SumOfPeopleVaccination)
AS
(
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(CONVERT(bigint, vac.new_vaccinations)) OVER (Partition by dea.location ORDER BY dea.location, dea.date) AS SumOfPeopleVaccination
FROM Projects..CovidDeaths dea
JOIN Projects..CovidVaccinations vac
	ON dea.location = vac.location AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
)
SELECT *,(SumOfPeopleVaccination/Population)*100 AS PercentOfVaccinations
FROM PopVsVac
ORDER BY 2,3


-- TEMP TABLE

DROP TABLE if exists #PercentPopulationVaccinated
CREATE TABLE #PercentPopulationVaccinated(
	Continet nvarchar(255), 
	Location nvarchar(255), 
	Date datetime, 
	Population numeric, 
	New_Vaccinations numeric, 
	SumOfPeopleVaccination numeric
)

INSERT INTO #PercentPopulationVaccinated
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(CONVERT(bigint, vac.new_vaccinations)) OVER (Partition by dea.location ORDER BY dea.location, dea.date) AS SumOfPeopleVaccination
FROM Projects..CovidDeaths dea
JOIN Projects..CovidVaccinations vac
	ON dea.location = vac.location AND dea.date = vac.date
WHERE dea.continent IS NOT NULL

SELECT *,(SumOfPeopleVaccination/Population)*100 AS PercentOfVaccinations
FROM #PercentPopulationVaccinated
ORDER BY 2,3


-- Creating View to store data for later visualizations

DROP VIEW if exists PercentPopulationVaccinated
GO
CREATE VIEW PercentPopulationVaccinated AS
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(CONVERT(bigint, vac.new_vaccinations)) OVER (Partition by dea.location ORDER BY dea.location, dea.date) AS SumOfPeopleVaccination
FROM Projects..CovidDeaths dea
JOIN Projects..CovidVaccinations vac
	ON dea.location = vac.location AND dea.date = vac.date
WHERE dea.continent IS NOT NULL

GO
SELECT *
FROM PercentPopulationVaccinated
ORDER BY 2,3














