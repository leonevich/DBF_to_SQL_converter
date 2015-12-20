package by.leonevich.converter.dbf;

import java.util.InputMismatchException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Load {

    public static void main(String[] args) {
        List<String> listFileName = ConverterDBF.getListFileName();
        if (listFileName.isEmpty()) {
            return;
        }
        Scanner scanner = new Scanner(System.in);
        ExecutorService executor = Executors.newFixedThreadPool(7);
        int next = -1;
        System.out.println("\nSelect file number to convert in SQL (0 - convert all files)");
        try {
            next = scanner.nextInt();
        } catch (InputMismatchException e) {
            System.out.println("Input only number");

        }
        if (next == 0) {
            for (String fileName : listFileName) {
                Runnable converter = new ConverterDBF(fileName);
                executor.execute(converter);
            }
        }

        if (next > 0 && next <= listFileName.size()) {
            Runnable converter = new ConverterDBF(listFileName.get(next - 1));
            executor.execute(converter);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        System.out.println("\nFinished");
    }
}
