QT = core
QT += sql

CONFIG += c++17 cmdline

SOURCES += \
        main.cpp \
        passBD/passdb.cpp

qnx: target.path = /tmp/$${TARGET}/bin
else: unix:!android: target.path = /opt/$${TARGET}/bin
!isEmpty(target.path): INSTALLS += target

DISTFILES += \
    parsak.py

HEADERS += \
    passBD/passdb.h
