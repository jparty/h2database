/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.Random;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.store.fs.FilePathCrypt;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.test.utils.FilePathUnstable;

/**
 * Tests the MVStore.
 */
public class TestKillProcessWhileWriting extends TestBase {

    private String fileName;
    private int seed;
    private FilePathUnstable fs;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        fs = FilePathUnstable.register();
        test("unstable:memFS:killProcess.h3");

        int todo;
        // need to test with a file system splits writes into blocks of 4 KB
        FilePathCrypt.register();
        test("unstable:crypt:0007:memFS:killProcess.h3");
    }

    private void test(String fileName) throws Exception {
        for (seed = 0; seed < 10; seed++) {
            this.fileName = fileName;
            FileUtils.delete(fileName);
            test(Integer.MAX_VALUE);
            int max = Integer.MAX_VALUE - fs.getDiskFullCount() + 10;
            assertTrue("" + (max - 10), max > 0);
            for (int i = 0; i < max; i++) {
                test(i);
            }
        }
    }

    private void test(int x) throws Exception {
        FileUtils.delete(fileName);
        fs.setDiskFullCount(x);
        try {
            write();
            verify();
        } catch (Exception e) {
            if (x == Integer.MAX_VALUE) {
                throw e;
            }
            fs.setDiskFullCount(0);
            verify();
        }
    }

    private int write() {
        MVStore s;
        MVMap<Integer, byte[]> m;

        s = new MVStore.Builder().
                fileName(fileName).
                pageSplitSize(50).
                writeDelay(0).
                open();
        s.setWriteDelay(0);
        m = s.openMap("data");
        Random r = new Random(seed);
        int op = 0;
        try {
            for (; op < 100; op++) {
                int k = r.nextInt(100);
                byte[] v = new byte[r.nextInt(100) * 100];
                int type = r.nextInt(10);
                switch (type) {
                case 0:
                case 1:
                case 2:
                case 3:
                    m.put(k, v);
                    break;
                case 4:
                case 5:
                    m.remove(k);
                    break;
                case 6:
                    s.store();
                    break;
                case 7:
                    s.compact(80);
                    break;
                case 8:
                    m.clear();
                    break;
                case 9:
                    s.close();
                    s = new MVStore.Builder().
                            fileName(fileName).
                            pageSplitSize(50).
                            writeDelay(0).open();
                    m = s.openMap("data");
                    break;
                }
            }
            s.store();
            s.close();
            return 0;
        } catch (Exception e) {
            s.closeImmediately();
            return op;
        }
    }

    private void verify() {

        MVStore s;
        MVMap<Integer, byte[]> m;

        FileUtils.delete(fileName);
        s = new MVStore.Builder().
                fileName(fileName).open();
        m = s.openMap("data");
        for (int i = 0; i < 100; i++) {
            byte[] x = m.get(i);
            if (x == null) {
                break;
            }
            assertEquals(i * 100, x.length);
        }
        s.close();
    }

}
