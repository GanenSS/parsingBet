#include <QCoreApplication>
#include "passBD/passdb.h"

int main(int argc, char *argv[])
{
    QCoreApplication a(argc, argv);

    passDB db;

    db.startParsingCycle();

    return a.exec();
}
