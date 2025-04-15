#ifndef PASSDB_H
#define PASSDB_H

#include <QObject>
#include <QSqlDatabase>
#include <QSqlQuery>
#include <QSqlTableModel>
#include <QSqlError>
#include <QVector>
#include <QJsonObject>
#include <QJsonArray>
#include <QDir>
#include <QFile>
#include <QJsonDocument>
#include <QDebug>
#include <QDateTime>
#include <QProcess>
#include <QTimer>

class passDB : public QObject
{
    Q_OBJECT
public:
    explicit passDB(QObject *parent = nullptr);
    ~passDB();

    struct Sport {
        int id;
        QString name;
    };

    struct Championship {
        int id;
        QString name;
        int sportId;
    };

    struct Match {
        int id;
        QString eventId;
        QString team1;
        QString team2;
        QString time;
        int championshipId;
        QString coefficient_first;
        QString coefficient_draw;
        QString coefficient_second;
        QString handicap1_value;
        QString handicap1_param;
        QString handicap2_value;
        QString handicap2_param;
        QString total_value;
        QString coefficient_over;
        QString coefficient_under;
    };

    struct Event {
        int id;
        int matchId;
        int parentEventId;
        QString eventId;
        QString name;
        QString time;
        QString description;
        QString coefficient_1;
        QString coefficient_X;
        QString coefficient_2;
        QString handicap1_value;
        QString handicap1_param;
        QString handicap2_value;
        QString handicap2_param;
        QString total_value;
        QString coefficient_over;
        QString coefficient_under;
    };

    void addSport(const Sport &sport);
    void addChampionship(const Championship &championship);
    void addMatch(const Match &match);
    void addEvent(const Event &event);
    void importJsonFile(const QString &filePath);
    void importAllJsonFiles(const QString &directoryPath);
    void clearAllTables();
    bool createConnection();

    bool startPythonParser();
    void startParsingCycle();

private slots:
    void handleParserFinished(int exitCode, QProcess::ExitStatus exitStatus);

private:
    QSqlDatabase db;
    QSqlQuery query;
    QProcess *parserProcess;
    QString parserDirectory;
    QString dataDirectory;

    const QString dbType = "QPSQL";
    const QString dbUserName = "postgres";
    const QString dbName = "BetNetiks";
    const QString dbPassword = "1+Qwertis+1";

    void log(const QString &message, const QString &level = "INFO");
    void processEvents(const QJsonArray &events, int matchId, int parentEventId);
};

#endif // PASSDB_H
