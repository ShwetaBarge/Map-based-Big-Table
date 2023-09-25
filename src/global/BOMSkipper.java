package global;

import java.io.IOException;
import java.io.Reader;

public class BOMSkipper {
    /**
     * Skips BOM character from the begning of the file
     * @param reader
     * @throws IOException
     */
    public static void skip(Reader reader) throws IOException
    {
        reader.mark(1);
        char[] possibleBOM = new char[1];
        reader.read(possibleBOM);

        if (possibleBOM[0] != '\ufeff') {
            reader.reset();
        }
    }
}
