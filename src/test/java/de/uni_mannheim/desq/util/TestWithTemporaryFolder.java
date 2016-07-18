package de.uni_mannheim.desq.util;

import org.junit.After;
import org.junit.Before;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 * Created by rgemulla on 18.07.2016.
 */
public class TestWithTemporaryFolder {
    private TemporaryFolder temporaryFolder;

    protected TestWithTemporaryFolder() {
    }

    @Before
    public void before() throws IOException {
        temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        System.out.println("Creating temporary folder " + temporaryFolder.getRoot());
    }

    @After
    public void after() {
        //temporaryFolder.delete();
        temporaryFolder = null;
    }

    protected File newTemporaryFile(String fileName) throws IOException {
        File file = temporaryFolder.newFile(fileName);
        System.out.println("Creating temporary file " + file);
        return file;
    }
}
