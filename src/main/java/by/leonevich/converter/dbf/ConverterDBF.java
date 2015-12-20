package by.leonevich.converter.dbf;

import com.hexiong.jdbf.DBFReader;
import com.hexiong.jdbf.JDBFException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class ConverterDBF implements Runnable {
    private static final String PATH_DIRECTORY = new File("").getAbsolutePath();
    private String fileName;
    private String pathFile;
    private long startTime = System.currentTimeMillis();

    public ConverterDBF() {
    }

    public ConverterDBF(String fileName) {
        this.pathFile = new File(fileName).getAbsolutePath();
        this.fileName = fileName.split("\\.")[0];
    }

    @Override
    public void run() {
        convertDbf(fileName);
    }

    private void convertDbf(String fileName) {
        DBFReader dbfreader;
        try {
            dbfreader = new DBFReader(pathFile);
        } catch (JDBFException | NullPointerException e) {
            System.out.println("\nBad file: " + fileName);
            return;
        }

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE TABLE " + "`").append(fileName).append("`").append(" (");
        int fieldCount = dbfreader.getFieldCount();
        List<String> list = new ArrayList<>(fieldCount);
        if (!(fieldCount < 3)) {
            for (int i = 0; i < fieldCount; i++) {
                String field = dbfreader.getField(i).getName();
                list.add(field);
                String z = i == (dbfreader.getFieldCount() - 1) ? ") " : ", ";
                stringBuilder.append("`").append(field).append("`").append(" varchar(255) not null").append(z);
            }
        }
        stringBuilder.append("ENGINE=InnoDB DEFAULT CHARACTER SET = utf8 DEFAULT COLLATE = utf8_general_ci");
        String insert = (";\nINSERT INTO " + "`" + fileName + "`" + " values \n");
        String prefix;
        String postfix;
        List<String> prefixList = new ArrayList<>();
        for (int i = 0; dbfreader.hasNextRecord(); i++) {
            Object aobj[] = new Object[0];
            try {
                aobj = dbfreader.nextRecord(Charset.forName("Cp866"));
            } catch (JDBFException e) {
                System.out.println(e.getMessage());
            }
            for (int j = 0; j < aobj.length; j++) {
                i = i > 4000 ? 0 : i;
                prefix = j == 0 && i == 0 ? "('" : (j == 0 ? ",\n('" : "'");
                postfix = j == aobj.length - 1 ? "')" : "', ";
                String append = i == 0 && j == 0 ? (insert + prefix + aobj[j] + postfix) : (prefix + aobj[j] + postfix);
                stringBuilder.append(append);
            }
        }
        stringBuilder.append(";");
        fileWrite(stringBuilder);
    }

    private void fileWrite(StringBuilder stringBuilder) {
        try (FileWriter writer = new FileWriter(pathFile.substring(0, pathFile.length() - 3) + "sql", false)) {
            writer.write(String.valueOf(stringBuilder));
            printStatus();
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private long printStatus() {
        System.out.print("\n" + fileName);
        long progressTime = (System.currentTimeMillis() - startTime);
        System.out.println(": " + progressTime + " ms");
        return progressTime;
    }

    public static List<String> getListFileName() {
        String fileName;
        File[] fList;
        String[] fileNameStr;
        File F = new File(PATH_DIRECTORY);
        List<String> listFileName = new ArrayList<>();
        fList = F.listFiles();

        assert fList != null;
        int i = 0;
        for (File aFList : fList) {
            if (aFList.isFile()) {
                fileName = aFList.getName();
                fileNameStr = fileName.split("\\.");
                if (!"dbf".equals(fileNameStr[1].toLowerCase()) || fileNameStr.length > 2) {
                    continue;
                } else {
                    ++i;
                }
                listFileName.add(fileName);
                System.out.println(i + " - " + fileName);
            }
        }
        if (listFileName.isEmpty()) {
            System.out.println("File not found");
        }
        return listFileName;
    }
}
