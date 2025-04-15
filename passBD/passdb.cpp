#include "passDB.h"

passDB::passDB(QObject *parent) : QObject(parent), parserProcess(nullptr)
{
    log("Initializing passDB object");
    createConnection();

    // Set the parser and data directories
    parserDirectory = "C:/Users/ezhak/Documents/parsingBet";
    dataDirectory = "C:/Users/ezhak/Documents/parsingBet/data";
}

passDB::~passDB()
{
    if (db.isOpen()) {
        log("Closing database connection");
        db.close();
    }

    if (parserProcess) {
        if (parserProcess->state() == QProcess::Running) {
            log("Terminating running parser process");
            parserProcess->terminate();
            parserProcess->waitForFinished(3000);
            if (parserProcess->state() == QProcess::Running) {
                parserProcess->kill();
            }
        }
        delete parserProcess;
    }

    log("passDB object destroyed");
}

void passDB::log(const QString &message, const QString &level)
{
    QString timestamp = QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss.zzz");
    qDebug() << QString("[%1] [%2] %3").arg(timestamp, level, message);
}

bool passDB::createConnection()
{
    log(QString("Attempting to connect to database. Type: %1, Name: %2, User: %3").arg(dbType, dbName, dbUserName));

    db = QSqlDatabase::addDatabase(dbType);
    db.setHostName("localhost");
    db.setDatabaseName(dbName);
    db.setUserName(dbUserName);
    db.setPassword(dbPassword);

    if (!db.open())
    {
        QString error = "Database connection error: " + db.lastError().text();
        log(error, "ERROR");
        return false;
    }
    else
    {
        log("Database connection established successfully");
        query = QSqlQuery(db);
        return true;
    }
}

bool passDB::startPythonParser()
{
    log("Starting Python parser");

    // Check if process is already running
    if (parserProcess && parserProcess->state() == QProcess::Running) {
        log("Parser is already running", "WARNING");
        return false;
    }

    // Create new process if needed
    if (!parserProcess) {
        parserProcess = new QProcess(this);
        connect(parserProcess, QOverload<int, QProcess::ExitStatus>::of(&QProcess::finished),
                this, &passDB::handleParserFinished);
    }

    // Set working directory
    parserProcess->setWorkingDirectory(parserDirectory);

    // Start the Python script
    log(QString("Running parser from directory: %1").arg(parserDirectory));
    parserProcess->start("python", QStringList() << "parsak.py");

    if (!parserProcess->waitForStarted(5000)) {
        log(QString("Failed to start Python parser: %1").arg(parserProcess->errorString()), "ERROR");
        return false;
    }

    log("Python parser started successfully");
    return true;
}

void passDB::handleParserFinished(int exitCode, QProcess::ExitStatus exitStatus)
{
    if (exitStatus == QProcess::CrashExit) {
        log("Parser process crashed!", "ERROR");
        log(parserProcess->readAllStandardError(), "ERROR");
    } else if (exitCode != 0) {
        log(QString("Parser process finished with error code: %1").arg(exitCode), "WARNING");
        log(parserProcess->readAllStandardError(), "WARNING");
    } else {
        log("Parser process finished successfully");
    }

    // Import the data after parser has finished
    log("Starting to import JSON data after parser completion");
    importAllJsonFiles(dataDirectory);

    // Start parser again to create a cycle
    QTimer::singleShot(500, this, &passDB::startPythonParser);
}

void passDB::startParsingCycle()
{
    log("Starting the parse-import cycle");
    startPythonParser();
}

void passDB::clearAllTables()
{
    log("Clearing all database tables before import");

    // Начинаем транзакцию для отключения ограничений
    query.exec("BEGIN");

    // Временно отключаем ограничения внешних ключей
    query.exec("ALTER TABLE matches DISABLE TRIGGER ALL");
    query.exec("ALTER TABLE championships DISABLE TRIGGER ALL");
    query.exec("ALTER TABLE sports DISABLE TRIGGER ALL");

    // Очищаем таблицы в обратном порядке
    query.exec("DELETE FROM matches");
    log("Matches table cleared");

    query.exec("DELETE FROM championships");
    log("Championships table cleared");

    query.exec("DELETE FROM sports");
    log("Sports table cleared");

    // Включаем обратно ограничения
    query.exec("ALTER TABLE matches ENABLE TRIGGER ALL");
    query.exec("ALTER TABLE championships ENABLE TRIGGER ALL");
    query.exec("ALTER TABLE sports ENABLE TRIGGER ALL");

    // Завершаем транзакцию
    query.exec("COMMIT");

    log("All tables cleared successfully");
}

void passDB::addSport(const Sport &sport)
{
    log(QString("Adding sport: ID=%1, Name=%2").arg(QString::number(sport.id), sport.name));

    query.prepare("INSERT INTO sports (sport_id, sport_name, sport_url) VALUES (?, ?, ?)");
    query.addBindValue(sport.id);
    query.addBindValue(sport.name);
    query.addBindValue(sport.url);

    if (!query.exec()) {
        QString error = QString("Failed to add sport (ID=%1): %2").arg(QString::number(sport.id), query.lastError().text());
        log(error, "ERROR");
    } else {
        log(QString("Successfully added sport (ID=%1)").arg(QString::number(sport.id)));
    }
}

void passDB::addChampionship(const Championship &championship)
{
    log(QString("Adding championship: ID=%1, Name=%2, SportID=%3").arg(
        QString::number(championship.id), championship.name, QString::number(championship.sportId)));

    query.prepare("INSERT INTO championships (championship_id, championship_name, sport_id) VALUES (?, ?, ?)");
    query.addBindValue(championship.id);
    query.addBindValue(championship.name);
    query.addBindValue(championship.sportId);

    if (!query.exec()) {
        QString error = QString("Failed to add championship (ID=%1): %2").arg(
            QString::number(championship.id), query.lastError().text());
        log(error, "ERROR");
    } else {
        log(QString("Successfully added championship (ID=%1)").arg(QString::number(championship.id)));
    }
}

void passDB::addMatch(const Match &match)
{
    log(QString("Adding match: ID=%1, Teams=%2 vs %3, ChampionshipID=%4").arg(
        QString::number(match.id), match.team1, match.team2, QString::number(match.championshipId)));

    // Обратите внимание, что мы используем тип TEXT для match_time, а не TIMESTAMP
    query.prepare("INSERT INTO matches (match_id, team1, team2, match_time, championship_id, "
                  "coefficient_first, coefficient_draw, coefficient_second, "
                  "coefficient_first_fora, coefficient_second_fora, "
                  "coefficient_total, coefficient_over, coefficient_under) "
                  "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

    query.addBindValue(match.id);
    query.addBindValue(match.team1);
    query.addBindValue(match.team2);
    query.addBindValue(match.time);  // Хранить как текстовую строку, а не конвертировать в timestamp
    query.addBindValue(match.championshipId);
    query.addBindValue(match.coefficient_first);
    query.addBindValue(match.coefficient_draw);
    query.addBindValue(match.coefficient_second);
    query.addBindValue(match.coefficient_first_fora);
    query.addBindValue(match.coefficient_second_fora);
    query.addBindValue(match.coefficient_total);
    query.addBindValue(match.coefficient_over);
    query.addBindValue(match.coefficient_under);

    if (!query.exec()) {
        QString error = QString("Failed to add match (ID=%1): %2").arg(
            QString::number(match.id), query.lastError().text());
        log(error, "ERROR");
    } else {
        log(QString("Successfully added match (ID=%1)").arg(QString::number(match.id)));
    }
}

void passDB::importJsonFile(const QString &filePath)
{
    log(QString("Importing JSON file: %1").arg(filePath));

    QFile file(filePath);
    if (!file.open(QIODevice::ReadOnly)) {
        log(QString("Failed to open file: %1").arg(filePath), "ERROR");
        return;
    }

    QByteArray jsonData = file.readAll();
    file.close();

    QJsonDocument jsonDoc = QJsonDocument::fromJson(jsonData);
    if (jsonDoc.isNull() || !jsonDoc.isObject()) {
        log(QString("Failed to parse JSON or root is not an object: %1").arg(filePath), "ERROR");
        return;
    }

    QJsonObject rootObject = jsonDoc.object();

    // Extract sport information
    int sportId = rootObject["sport_id"].toInt();
    QString sportName = rootObject["sport_name"].toString();
    QString sportUrl = rootObject["url"].toString();

    log(QString("Processing sport: ID=%1, Name=%2").arg(QString::number(sportId), sportName));

    Sport sport = { sportId, sportName, sportUrl };
    addSport(sport);

    // Process championships
    if (rootObject.contains("championships") && rootObject["championships"].isArray()) {
        QJsonArray championshipsArray = rootObject["championships"].toArray();
        log(QString("Found %1 championships for sport ID=%2").arg(
            QString::number(championshipsArray.size()), QString::number(sportId)));

        for (const QJsonValue &championshipValue : championshipsArray) {
            if (championshipValue.isObject()) {
                QJsonObject championshipObject = championshipValue.toObject();
                int championshipId = championshipObject["championship_id"].toInt();
                QString championshipName = championshipObject["championship_name"].toString();

                log(QString("Processing championship: ID=%1, Name=%2").arg(
                    QString::number(championshipId), championshipName));

                Championship championship = { championshipId, championshipName, sportId };
                addChampionship(championship);

                if (championshipObject.contains("matches") && championshipObject["matches"].isArray()) {
                    QJsonArray matchesArray = championshipObject["matches"].toArray();
                    log(QString("Found %1 matches for championship ID=%2").arg(
                        QString::number(matchesArray.size()), QString::number(championshipId)));

                    for (const QJsonValue &matchValue : matchesArray) {
                        if (matchValue.isObject()) {
                            QJsonObject matchObject = matchValue.toObject();

                            int matchId = matchObject["match_id"].toInt();
                            QString team1 = matchObject["team1"].toString();
                            QString team2 = matchObject["team2"].toString();
                            QString time = matchObject["time"].toString();

                            log(QString("Processing match: ID=%1, %2 vs %3").arg(
                                QString::number(matchId), team1, team2));

                            // Extract odds from the odds object
                            QJsonObject oddsObject = matchObject["odds"].toObject();

                            QString coef1 = "-";
                            QString coefX = "-";
                            QString coef2 = "-";
                            QString coefFora1 = "-";
                            QString coefFora2 = "-";
                            QString coefTotal = "-";
                            QString coefOver = "-";
                            QString coefUnder = "-";

                            // Convert odds values to strings, handling various formats
                            if (oddsObject.contains("1") && !oddsObject["1"].isNull()) {
                                if (oddsObject["1"].isDouble())
                                    coef1 = QString::number(oddsObject["1"].toDouble());
                                else
                                    coef1 = oddsObject["1"].toString();
                            }

                            if (oddsObject.contains("X") && !oddsObject["X"].isNull()) {
                                if (oddsObject["X"].isDouble())
                                    coefX = QString::number(oddsObject["X"].toDouble());
                                else
                                    coefX = oddsObject["X"].toString();
                            }

                            if (oddsObject.contains("2") && !oddsObject["2"].isNull()) {
                                if (oddsObject["2"].isDouble())
                                    coef2 = QString::number(oddsObject["2"].toDouble());
                                else
                                    coef2 = oddsObject["2"].toString();
                            }

                            // Handle handicap/fora data
                            if (oddsObject.contains("HANDICAP 1") && oddsObject["HANDICAP 1"].isObject()) {
                                QJsonObject fora1 = oddsObject["HANDICAP 1"].toObject();
                                QString value = fora1["value"].toString();
                                if (fora1["value"].isDouble()) {
                                    value = QString::number(fora1["value"].toDouble());
                                }
                                QString param = fora1["param"].toString();
                                coefFora1 = value + " (" + param + ")";
                            } else if (oddsObject.contains("ФОРА 1") && oddsObject["ФОРА 1"].isObject()) {
                                QJsonObject fora1 = oddsObject["ФОРА 1"].toObject();
                                QString value = fora1["value"].toString();
                                if (fora1["value"].isDouble()) {
                                    value = QString::number(fora1["value"].toDouble());
                                }
                                QString param = fora1["param"].toString();
                                coefFora1 = value + " (" + param + ")";
                            }

                            if (oddsObject.contains("HANDICAP 2") && oddsObject["HANDICAP 2"].isObject()) {
                                QJsonObject fora2 = oddsObject["HANDICAP 2"].toObject();
                                QString value = fora2["value"].toString();
                                if (fora2["value"].isDouble()) {
                                    value = QString::number(fora2["value"].toDouble());
                                }
                                QString param = fora2["param"].toString();
                                coefFora2 = value + " (" + param + ")";
                            } else if (oddsObject.contains("ФОРА 2") && oddsObject["ФОРА 2"].isObject()) {
                                QJsonObject fora2 = oddsObject["ФОРА 2"].toObject();
                                QString value = fora2["value"].toString();
                                if (fora2["value"].isDouble()) {
                                    value = QString::number(fora2["value"].toDouble());
                                }
                                QString param = fora2["param"].toString();
                                coefFora2 = value + " (" + param + ")";
                            }

                            // Handle total
                            if (oddsObject.contains("TOTAL"))
                                coefTotal = oddsObject["TOTAL"].toString();

                            if (oddsObject.contains("OVER")) {
                                if (oddsObject["OVER"].isDouble())
                                    coefOver = QString::number(oddsObject["OVER"].toDouble());
                                else
                                    coefOver = oddsObject["OVER"].toString();
                            } else if (oddsObject.contains("Б")) {
                                if (oddsObject["Б"].isDouble())
                                    coefOver = QString::number(oddsObject["Б"].toDouble());
                                else
                                    coefOver = oddsObject["Б"].toString();
                            }

                            if (oddsObject.contains("UNDER")) {
                                if (oddsObject["UNDER"].isDouble())
                                    coefUnder = QString::number(oddsObject["UNDER"].toDouble());
                                else
                                    coefUnder = oddsObject["UNDER"].toString();
                            } else if (oddsObject.contains("М")) {
                                if (oddsObject["М"].isDouble())
                                    coefUnder = QString::number(oddsObject["М"].toDouble());
                                else
                                    coefUnder = oddsObject["М"].toString();
                            }

                            Match match = {
                                matchId,
                                team1,
                                team2,
                                time,
                                championshipId,
                                coef1,
                                coefX,
                                coef2,
                                coefFora1,
                                coefFora2,
                                coefTotal,
                                coefOver,
                                coefUnder
                            };

                            addMatch(match);
                        }
                    }
                }
            }
        }
    }

    log(QString("Finished processing file: %1").arg(filePath));
}

void passDB::importAllJsonFiles(const QString &directoryPath)
{
    log(QString("Starting import of all JSON files from directory: %1").arg(directoryPath));

    QDir directory(directoryPath);
    if (!directory.exists()) {
        log(QString("Directory not found: %1").arg(directoryPath), "ERROR");
        return;
    }

    // Clear all tables before importing new data
    clearAllTables();

    QStringList jsonFiles = directory.entryList(QStringList() << "*.json", QDir::Files);
    log(QString("Found %1 JSON files in directory").arg(QString::number(jsonFiles.count())));

    for (const QString &filename : jsonFiles)
    {
        QString filePath = directory.absoluteFilePath(filename);
        log(QString("Importing file: %1").arg(filePath));
        importJsonFile(filePath);
    }

    log("Finished importing all JSON files");
}
