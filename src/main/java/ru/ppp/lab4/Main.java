package ru.ppp.lab4;

import ru.ppp.lab4.build.BuildJob;
import ru.ppp.lab4.query.QueryJob;

import java.util.Arrays;

/** Точка входа: build | query. */
public final class Main {

    private Main() {
    }

    public static void main(String[] argv) {
        if (argv.length == 0) {
            usage();
            System.exit(2);
        }

        int code;
        switch (argv[0]) {
            case "build":
                code = new BuildJob().run(Arrays.copyOfRange(argv, 1, argv.length));
                break;
            case "query":
                code = new QueryJob().run(Arrays.copyOfRange(argv, 1, argv.length));
                break;
            default:
                System.err.println("неизвестная команда: " + argv[0]);
                usage();
                code = 2;
        }
        System.exit(code);
    }

    private static void usage() {
        System.err.println("использование:");
        System.err.println("  build [--stats PATH] [--players PATH] [--salaries PATH] [--output PATH]");
        System.err.println("  query --season YYYY [--type top|undervalued] [--limit N]"
                + " [--min-games 20] [--data PATH] [--explain]");
    }
}
