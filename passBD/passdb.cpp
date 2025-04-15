#include "passdb.h"

passDB::passDB(QObject *parent) : QObject(parent), parserProcess(nullptr)
{
    log("Инициализация объекта passDB");
    createConnection();

    parserDirectory = "C:/Users/ezhak/Documents/parsingBet";
    dataDirectory = "C:/Users/ezhak/Documents/parsingBet/data";

    QDir dir(dataDirectory);
    if (!dir.exists()) {
        log("Создание директории для данных: " + dataDirectory);
        if (dir.mkpath(dataDirectory)) {
            log("Директория успешно создана");
        } else {
            log("Ошибка при создании директории", "ERROR");
        }
    }
}

passDB::~passDB()
{
    if (db.isOpen()) {
        log("Закрытие подключения к базе данных");
        db.close();
    }

    if (parserProcess) {
        if (parserProcess->state() == QProcess::Running) {
            log("Завершение работы запущенного парсера");
            parserProcess->terminate();
            parserProcess->waitForFinished(3000);
            if (parserProcess->state() == QProcess::Running) {
                parserProcess->kill();
            }
        }
        delete parserProcess;
    }

    log("Объект passDB уничтожен");
}

void passDB::log(const QString &message, const QString &level)
{
    QString timestamp = QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss.zzz");
    qDebug() << QString("[%1] [%2] %3").arg(timestamp, level, message);
}

bool passDB::createConnection()
{
    log("Попытка подключения к базе данных...");

    db = QSqlDatabase::addDatabase(dbType);
    db.setHostName("localhost");
    db.setDatabaseName(dbName);
    db.setUserName(dbUserName);
    db.setPassword(dbPassword);

    if (!db.open())
    {
        log("Ошибка подключения к базе данных: " + db.lastError().text(), "ERROR");
        return false;
    }
    else
    {
        log("Подключение к базе данных успешно установлено");
        query = QSqlQuery(db);
        return true;
    }
}

bool passDB::startPythonParser()
{
    log("Запуск Python парсера");

    if (parserProcess && parserProcess->state() == QProcess::Running) {
        log("Парсер уже запущен", "WARNING");
        return false;
    }

    if (!parserProcess) {
        parserProcess = new QProcess(this);
        connect(parserProcess, QOverload<int, QProcess::ExitStatus>::of(&QProcess::finished),
                this, &passDB::handleParserFinished);
    }

    parserProcess->setWorkingDirectory(parserDirectory);
    parserProcess->start("python", QStringList() << "parsak.py");

    if (!parserProcess->waitForStarted(5000)) {
        log("Ошибка запуска Python парсера: " + parserProcess->errorString(), "ERROR");
        return false;
    }

    log("Python парсер успешно запущен");
    return true;
}

void passDB::handleParserFinished(int exitCode, QProcess::ExitStatus exitStatus)
{
    if (exitStatus == QProcess::CrashExit) {
        log("Критическая ошибка в работе парсера!", "ERROR");
        log(parserProcess->readAllStandardError(), "ERROR");
    } else if (exitCode != 0) {
        log("Парсер завершился с ошибкой: " + QString::number(exitCode), "WARNING");
    } else {
        log("Парсер успешно завершил работу");
    }

    log("Начинаем импорт данных");
    importAllJsonFiles(dataDirectory);

    QTimer::singleShot(300000, this, &passDB::startPythonParser);
}

void passDB::startParsingCycle()
{
    log("Запуск цикла парсинга и импорта");
    startPythonParser();
}

void passDB::clearAllTables()
{
    log("Очистка всех таблиц базы данных перед импортом");

    query.exec("BEGIN");

    query.exec("ALTER TABLE match_events DISABLE TRIGGER ALL");
    query.exec("ALTER TABLE matches DISABLE TRIGGER ALL");
    query.exec("ALTER TABLE championships DISABLE TRIGGER ALL");
    query.exec("ALTER TABLE sports DISABLE TRIGGER ALL");

    query.exec("DELETE FROM match_events");
    log("Таблица match_events очищена");

    query.exec("DELETE FROM matches");
    log("Таблица matches очищена");

    query.exec("DELETE FROM championships");
    log("Таблица championships очищена");

    query.exec("DELETE FROM sports");
    log("Таблица sports очищена");

    query.exec("ALTER TABLE match_events ENABLE TRIGGER ALL");
    query.exec("ALTER TABLE matches ENABLE TRIGGER ALL");
    query.exec("ALTER TABLE championships ENABLE TRIGGER ALL");
    query.exec("ALTER TABLE sports ENABLE TRIGGER ALL");

    query.exec("COMMIT");

    log("Все таблицы успешно очищены");
}

void passDB::addSport(const Sport &sport)
{
    log(QString("Добавление вида спорта: ID=%1, Name=%2").arg(QString::number(sport.id), sport.name));

    query.prepare("INSERT INTO sports (sport_id, sport_name) VALUES (?, ?)");
    query.addBindValue(sport.id);
    query.addBindValue(sport.name);

    if (!query.exec()) {
        QString error = QString("Ошибка при добавлении вида спорта (ID=%1): %2").arg(QString::number(sport.id), query.lastError().text());
        log(error, "ERROR");
    } else {
        log(QString("Вид спорта успешно добавлен (ID=%1)").arg(QString::number(sport.id)));
    }
}

void passDB::addChampionship(const Championship &championship)
{
    log(QString("Добавление чемпионата: ID=%1, Name=%2, SportID=%3").arg(
        QString::number(championship.id), championship.name, QString::number(championship.sportId)));

    query.prepare("INSERT INTO championships (championship_id, championship_name, sport_id) VALUES (?, ?, ?)");
    query.addBindValue(championship.id);
    query.addBindValue(championship.name);
    query.addBindValue(championship.sportId);

    if (!query.exec()) {
        QString error = QString("Ошибка при добавлении чемпионата (ID=%1): %2").arg(
            QString::number(championship.id), query.lastError().text());
        log(error, "ERROR");
    } else {
        log(QString("Чемпионат успешно добавлен (ID=%1)").arg(QString::number(championship.id)));
    }
}

void passDB::addMatch(const Match &match)
{
    log(QString("Добавление матча: ID=%1, Teams=%2 vs %3, ChampionshipID=%4").arg(
        QString::number(match.id), match.team1, match.team2, QString::number(match.championshipId)));

    query.prepare("INSERT INTO matches (match_id, event_id, team1, team2, match_time, championship_id, "
                  "coefficient_first, coefficient_draw, coefficient_second, "
                  "handicap1_value, handicap1_param, handicap2_value, handicap2_param, "
                  "total_value, coefficient_over, coefficient_under) "
                  "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

    query.addBindValue(match.id);
    query.addBindValue(match.eventId);
    query.addBindValue(match.team1);
    query.addBindValue(match.team2);
    query.addBindValue(match.time);
    query.addBindValue(match.championshipId);
    query.addBindValue(match.coefficient_first);
    query.addBindValue(match.coefficient_draw);
    query.addBindValue(match.coefficient_second);
    query.addBindValue(match.handicap1_value);
    query.addBindValue(match.handicap1_param);
    query.addBindValue(match.handicap2_value);
    query.addBindValue(match.handicap2_param);
    query.addBindValue(match.total_value);
    query.addBindValue(match.coefficient_over);
    query.addBindValue(match.coefficient_under);

    if (!query.exec()) {
        QString error = QString("Ошибка при добавлении матча (ID=%1): %2").arg(
            QString::number(match.id), query.lastError().text());
        log(error, "ERROR");
    } else {
        log(QString("Матч успешно добавлен (ID=%1)").arg(QString::number(match.id)));
    }
}

void passDB::addEvent(const Event &event)
{
    log(QString("Добавление события: ID=%1, Name=%2, MatchID=%3, ParentID=%4").arg(
        QString::number(event.id), event.name, QString::number(event.matchId),
        event.parentEventId > 0 ? QString::number(event.parentEventId) : "NULL"));

    query.prepare("INSERT INTO match_events "
                  "(event_id, match_id, parent_event_id, event_name, event_time, event_description, "
                  "coefficient_1, coefficient_X, coefficient_2, "
                  "handicap1_value, handicap1_param, handicap2_value, handicap2_param, "
                  "total_value, coefficient_over, coefficient_under) "
                  "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

    query.addBindValue(event.id);
    query.addBindValue(event.matchId);
    query.addBindValue(event.parentEventId > 0 ? event.parentEventId : QVariant(QVariant::Int));
    query.addBindValue(event.name);
    query.addBindValue(event.time);
    query.addBindValue(event.description);
    query.addBindValue(event.coefficient_1);
    query.addBindValue(event.coefficient_X);
    query.addBindValue(event.coefficient_2);
    query.addBindValue(event.handicap1_value);
    query.addBindValue(event.handicap1_param);
    query.addBindValue(event.handicap2_value);
    query.addBindValue(event.handicap2_param);
    query.addBindValue(event.total_value);
    query.addBindValue(event.coefficient_over);
    query.addBindValue(event.coefficient_under);

    if (!query.exec()) {
        QString error = QString("Ошибка при добавлении события (ID=%1): %2").arg(
            QString::number(event.id), query.lastError().text());
        log(error, "ERROR");
    } else {
        log(QString("Событие успешно добавлено (ID=%1)").arg(QString::number(event.id)));
    }
}

void passDB::processEvents(const QJsonArray &events, int matchId, int parentEventId)
{
    static int eventIdCounter = 1000000;

    for (const QJsonValue &eventValue : events) {
        if (eventValue.isObject()) {
            QJsonObject eventObject = eventValue.toObject();

            QString eventId = eventObject["eventId"].toString();
            QString name = eventObject["name"].toString();
            QString time = eventObject["time"].toString();
            QString description = eventObject["description"].toString();

            QJsonObject oddsObject = eventObject["odds"].toObject();

            QString coef1 = "-";
            QString coefX = "-";
            QString coef2 = "-";
            QString handicap1Value = "-";
            QString handicap1Param = "-";
            QString handicap2Value = "-";
            QString handicap2Param = "-";
            QString totalValue = "-";
            QString coefOver = "-";
            QString coefUnder = "-";

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

            if (oddsObject.contains("HANDICAP 1") && oddsObject["HANDICAP 1"].isObject()) {
                QJsonObject handicap1 = oddsObject["HANDICAP 1"].toObject();
                if (handicap1["value"].isDouble()) {
                    handicap1Value = QString::number(handicap1["value"].toDouble());
                } else {
                    handicap1Value = handicap1["value"].toString();
                }
                handicap1Param = handicap1["param"].toString();
            }

            if (oddsObject.contains("HANDICAP 2") && oddsObject["HANDICAP 2"].isObject()) {
                QJsonObject handicap2 = oddsObject["HANDICAP 2"].toObject();
                if (handicap2["value"].isDouble()) {
                    handicap2Value = QString::number(handicap2["value"].toDouble());
                } else {
                    handicap2Value = handicap2["value"].toString();
                }
                handicap2Param = handicap2["param"].toString();
            }

            if (oddsObject.contains("TOTAL"))
                totalValue = oddsObject["TOTAL"].toString();

            if (oddsObject.contains("OVER")) {
                if (oddsObject["OVER"].isDouble())
                    coefOver = QString::number(oddsObject["OVER"].toDouble());
                else
                    coefOver = oddsObject["OVER"].toString();
            }

            if (oddsObject.contains("UNDER")) {
                if (oddsObject["UNDER"].isDouble())
                    coefUnder = QString::number(oddsObject["UNDER"].toDouble());
                else
                    coefUnder = oddsObject["UNDER"].toString();
            }

            Event event;
            event.id = eventIdCounter++;
            event.eventId = eventId;
            event.matchId = matchId;
            event.parentEventId = parentEventId;
            event.name = name;
            event.time = time;
            event.description = description;
            event.coefficient_1 = coef1;
            event.coefficient_X = coefX;
            event.coefficient_2 = coef2;
            event.handicap1_value = handicap1Value;
            event.handicap1_param = handicap1Param;
            event.handicap2_value = handicap2Value;
            event.handicap2_param = handicap2Param;
            event.total_value = totalValue;
            event.coefficient_over = coefOver;
            event.coefficient_under = coefUnder;

            addEvent(event);

            if (eventObject.contains("subEvents") && eventObject["subEvents"].isArray()) {
                processEvents(eventObject["subEvents"].toArray(), matchId, event.id);
            }
        }
    }
}

void passDB::importJsonFile(const QString &filePath)
{
    log(QString("Импорт JSON файла: %1").arg(filePath));

    QFile file(filePath);
    if (!file.open(QIODevice::ReadOnly)) {
        log(QString("Ошибка открытия файла: %1").arg(filePath), "ERROR");
        return;
    }

    QByteArray jsonData = file.readAll();
    file.close();

    QJsonDocument jsonDoc = QJsonDocument::fromJson(jsonData);
    if (jsonDoc.isNull() || !jsonDoc.isObject()) {
        log(QString("Ошибка парсинга JSON или корень не является объектом: %1").arg(filePath), "ERROR");
        return;
    }

    QJsonObject rootObject = jsonDoc.object();

    if (!rootObject.contains("sport") || !rootObject["sport"].isObject()) {
        log(QString("Неверная структура JSON: отсутствует объект sport: %1").arg(filePath), "ERROR");
        return;
    }

    QJsonObject sportObject = rootObject["sport"].toObject();

    int sportId = sportObject["sportId"].toString().toInt();
    QString sportName = sportObject["sportName"].toString();

    log(QString("Обработка вида спорта: ID=%1, Name=%2").arg(QString::number(sportId), sportName));

    Sport sport = { sportId, sportName };
    addSport(sport);

    if (sportObject.contains("championships") && sportObject["championships"].isArray()) {
        QJsonArray championshipsArray = sportObject["championships"].toArray();
        log(QString("Найдено %1 чемпионатов для вида спорта ID=%2").arg(
            QString::number(championshipsArray.size()), QString::number(sportId)));

        for (const QJsonValue &championshipValue : championshipsArray) {
            if (championshipValue.isObject()) {
                QJsonObject championshipObject = championshipValue.toObject();
                int championshipId = championshipObject["championshipId"].toString().toInt();
                QString championshipName = championshipObject["championshipName"].toString();

                log(QString("Обработка чемпионата: ID=%1, Name=%2").arg(
                    QString::number(championshipId), championshipName));

                Championship championship = { championshipId, championshipName, sportId };
                addChampionship(championship);

                if (championshipObject.contains("matches") && championshipObject["matches"].isArray()) {
                    QJsonArray matchesArray = championshipObject["matches"].toArray();
                    log(QString("Найдено %1 матчей для чемпионата ID=%2").arg(
                        QString::number(matchesArray.size()), QString::number(championshipId)));

                    static int matchIdCounter = 100000;

                    for (const QJsonValue &matchValue : matchesArray) {
                        if (matchValue.isObject()) {
                            QJsonObject matchObject = matchValue.toObject();

                            QString eventId = matchObject["eventId"].toString();
                            QString team1 = matchObject["team1"].toString();
                            QString team2 = matchObject["team2"].toString();
                            QString time = matchObject["time"].toString();

                            int matchId = matchIdCounter++;

                            log(QString("Обработка матча: ID=%1, EventID=%2, %3 vs %4").arg(
                                QString::number(matchId), eventId, team1, team2));

                            QJsonObject oddsObject = matchObject["odds"].toObject();

                            QString coef1 = "-";
                            QString coefX = "-";
                            QString coef2 = "-";
                            QString handicap1Value = "-";
                            QString handicap1Param = "-";
                            QString handicap2Value = "-";
                            QString handicap2Param = "-";
                            QString totalValue = "-";
                            QString coefOver = "-";
                            QString coefUnder = "-";

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

                            if (oddsObject.contains("HANDICAP 1") && oddsObject["HANDICAP 1"].isObject()) {
                                QJsonObject handicap1 = oddsObject["HANDICAP 1"].toObject();
                                if (handicap1["value"].isDouble()) {
                                    handicap1Value = QString::number(handicap1["value"].toDouble());
                                } else {
                                    handicap1Value = handicap1["value"].toString();
                                }
                                handicap1Param = handicap1["param"].toString();
                            }

                            if (oddsObject.contains("HANDICAP 2") && oddsObject["HANDICAP 2"].isObject()) {
                                QJsonObject handicap2 = oddsObject["HANDICAP 2"].toObject();
                                if (handicap2["value"].isDouble()) {
                                    handicap2Value = QString::number(handicap2["value"].toDouble());
                                } else {
                                    handicap2Value = handicap2["value"].toString();
                                }
                                handicap2Param = handicap2["param"].toString();
                            }

                            if (oddsObject.contains("TOTAL"))
                                totalValue = oddsObject["TOTAL"].toString();

                            if (oddsObject.contains("OVER")) {
                                if (oddsObject["OVER"].isDouble())
                                    coefOver = QString::number(oddsObject["OVER"].toDouble());
                                else
                                    coefOver = oddsObject["OVER"].toString();
                            }

                            if (oddsObject.contains("UNDER")) {
                                if (oddsObject["UNDER"].isDouble())
                                    coefUnder = QString::number(oddsObject["UNDER"].toDouble());
                                else
                                    coefUnder = oddsObject["UNDER"].toString();
                            }

                            Match match = {
                                matchId,
                                eventId,
                                team1,
                                team2,
                                time,
                                championshipId,
                                coef1,
                                coefX,
                                coef2,
                                handicap1Value,
                                handicap1Param,
                                handicap2Value,
                                handicap2Param,
                                totalValue,
                                coefOver,
                                coefUnder
                            };

                            addMatch(match);

                            if (matchObject.contains("events") && matchObject["events"].isArray()) {
                                QJsonArray eventsArray = matchObject["events"].toArray();
                                log(QString("Найдено %1 событий для матча ID=%2").arg(
                                    QString::number(eventsArray.size()), QString::number(matchId)));

                                processEvents(eventsArray, matchId, 0);
                            }
                        }
                    }
                }
            }
        }
    }

    log(QString("Завершена обработка файла: %1").arg(filePath));
}

void passDB::importAllJsonFiles(const QString &directoryPath)
{
    log(QString("Начинается импорт всех JSON файлов из директории: %1").arg(directoryPath));

    QDir directory(directoryPath);
    if (!directory.exists()) {
        log(QString("Директория не найдена: %1").arg(directoryPath), "ERROR");
        return;
    }

    clearAllTables();

    QStringList jsonFiles = directory.entryList(QStringList() << "*.json", QDir::Files);
    log(QString("Найдено %1 JSON файлов в директории").arg(QString::number(jsonFiles.count())));

    for (const QString &filename : jsonFiles)
    {
        QString filePath = directory.absoluteFilePath(filename);
        log(QString("Импорт файла: %1").arg(filePath));
        importJsonFile(filePath);
    }

    log("Завершен импорт всех JSON файлов");
}
