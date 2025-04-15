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

    bool createConnection();

    struct Sport {
        int id;      // ID as int
        QString name; // Sport name
        QString url;  // Sport URL
    };

    struct Championship {
        int id;          // Championship ID as int
        QString name;    // Championship name
        int sportId;     // Sport ID as int
    };

    struct Match {
        int id;                       // Match ID as int
        QString team1;                // Team 1
        QString team2;                // Team 2
        QString time;                 // Match time
        int championshipId;           // Championship ID as int
        QString coefficient_first;     // Coefficient for first team win
        QString coefficient_draw;      // Coefficient for draw
        QString coefficient_second;    // Coefficient for second team win
        QString coefficient_first_fora; // Handicap coefficient for first team
        QString coefficient_second_fora; // Handicap coefficient for second team
        QString coefficient_total;     // Total coefficient
        QString coefficient_over;      // Over total coefficient
        QString coefficient_under;     // Under total coefficient
    };

    void addSport(const Sport &sport);
    void addChampionship(const Championship &championship);
    void addMatch(const Match &match);
    void importJsonFile(const QString &filePath);
    void importAllJsonFiles(const QString &directoryPath);
    void clearAllTables(); // Added new method to clear tables

    // New functions for parser integration
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
};

#endif // PASSDB_H
