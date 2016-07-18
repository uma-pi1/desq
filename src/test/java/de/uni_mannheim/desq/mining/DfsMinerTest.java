package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.util.TestUtils;
import de.uni_mannheim.desq.util.TestWithTemporaryFolder;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.InvocationTargetException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by rgemulla on 16.7.2016.
 */
public class DfsMinerTest extends TestWithTemporaryFolder {
    @Test
    public void icdm() throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        int sigma, gamma, lambda;
        boolean generalize;
        String fileName;

        sigma = 2; gamma = 0; lambda = 3; generalize = true;
        fileName = "icdm16-example-patterns-ids-" + sigma + "-" + gamma + "-" + lambda + "-" + generalize + ".del";
        File actualFile = newTemporaryFile(fileName);
        MinerTestUtils.mineIcdm16Example(DfsMiner.class, actualFile,
                DfsMiner.createProperties(sigma, gamma, lambda, generalize));
        assertThat(actualFile).hasSameContentAs(TestUtils.getPackageResource(getClass(), fileName));
    }
}
