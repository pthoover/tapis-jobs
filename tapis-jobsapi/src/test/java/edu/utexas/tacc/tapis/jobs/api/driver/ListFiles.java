package edu.utexas.tacc.tapis.jobs.api.driver;

import java.io.IOException;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.client.FilesClient;

public class ListFiles 
{
    // ***************** Configuration Parameters *****************
    private static final String BASE_URL = "https://dev.develop.tapis.io";
    private static final String userJWT = 
            "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJqdGkiOiIyNDExNGM0Yy0xZDAxLTQ3YjgtYTM2Yy05ZWRmZThlMmIzMmUiLCJpc3MiOiJodHRwczovL2Rldi5kZXZlbG9wLnRhcGlzLmlvL3YzL3Rva2VucyIsInN1YiI6InRlc3R1c2VyMkBkZXYiLCJ0YXBpcy90ZW5hbnRfaWQiOiJkZXYiLCJ0YXBpcy90b2tlbl90eXBlIjoiYWNjZXNzIiwidGFwaXMvZGVsZWdhdGlvbiI6ZmFsc2UsInRhcGlzL2RlbGVnYXRpb25fc3ViIjpudWxsLCJ0YXBpcy91c2VybmFtZSI6InRlc3R1c2VyMiIsInRhcGlzL2FjY291bnRfdHlwZSI6InVzZXIiLCJleHAiOjE5NDI2OTEwOTV9.APiY4sFPoNyCBIRxcUIzpastDHxM-msiaiynYw3_lTcIJdAdRRzejVOuY61ZX0F61D4yS5zVakfiyTQJtP8y_4COW3BFS8T0zojaiQN6LF0Pp-RYD9M5p58LzXHGHDnVe8MyVkQZMqTyqvxZpLISQJ-vo3iyEEid0_z0OE4Ks_sJh-2McwsjiKIgnDOIVHNkXULKoORpcfWuhjNMZ5Ezj_uUdVwAmEsYvCDj48B0o94JXBtKOlUG-nQkLn4CYBUmkGGZtmiK3XcFxu0uqhBRYjohK219H9xPzrVsVj8MBU1T76FW6ycbZ8jAGsr6Hs3oOY4ok75tnZ90-ncbhADi6w";
  //        "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJqdGkiOiIxOTVhNzU2YS1hZWRhLTQ2NjQtODFhMS1hODg1ZDZhZmJiNDMiLCJpc3MiOiJodHRwczovL2Rldi5kZXZlbG9wLnRhcGlzLmlvL3YzL3Rva2VucyIsInN1YiI6InRlc3R1c2VyMkBkZXYiLCJ0YXBpcy90ZW5hbnRfaWQiOiJkZXYiLCJ0YXBpcy90b2tlbl90eXBlIjoiYWNjZXNzIiwidGFwaXMvZGVsZWdhdGlvbiI6ZmFsc2UsInRhcGlzL2RlbGVnYXRpb25fc3ViIjpudWxsLCJ0YXBpcy91c2VybmFtZSI6InRlc3R1c2VyMiIsInRhcGlzL2FjY291bnRfdHlwZSI6InVzZXIiLCJleHAiOjE5MjU4MzgyMzF9.wbyeWa6PQpROtnPWpykKc9ln2TQj04cD_uwjS40UeF5PMDJ7jd5u8GJ0JPyaH-qj9R3H9-J4H9vQGPnKQg7Wqj9_QIja9t5g5WM7Vz70TaXmu91EO3_rbJkmguXZMRFdBS0YFDYGLccO2i50NVyt3i-nVRAp3nFCn5-eB6UEoU_KEe5MiFnMmuzF6kUIGDi6Cw_26DxI_SsY-zcpjCmX0jx5cM0xqLv8XNv1RIVr8o9fKGuvGupdT0ZdTCp_MiMBPi11OE7OCYo7iwp-yglcpOMlQF8LOCJe9txzJGqcCZSGbQBi4mNLasyYn0cstVak9ToGQpEpiSdz4FvQtSKT9A";
    
    // System id.
    private static final String EXEC_SYSTEM = "tapisv3-exec2";
    // ***************** End Configuration Parameters *************
    
    /** Driver
     * 
     * @throws Exception on error
     */
    public static void main(String[] args) throws Exception
    {
        // Try to submit a job.
        var listFiles = new ListFiles();
        listFiles.get(args);
    }
    
    /** Get a list of files on the system.
     * 
     * The list api behavior is:
     * 
     *   - Parameters:  String systemId, String path, int limit, long offset, boolean meta
     * 
     *   - name is the simple name (last component of a pathname)
     *   - input path is the path relative to the rootDir of system
     *      - if the input path includes a leading /, then the returned path will also
     *      - if the input path does not have a leading /, then the returned path will not
     *          - the empty string is ok, it specifies the rootDir just like / 
     *   - if the input path is a directory, it is not listed in the output
     *   - if the input path is a file, it is listed in the output (only item returned)
     *   - it's not clear what effect the meta flag has in posix file systems
     * 
     * @param args contains the name of a request file
     * @throws IOException 
     * @throws TapisClientException 
     */
    public void get(String[] args) throws IOException, TapisClientException
    {
        // Create the app.
        var filesClient = new FilesClient(BASE_URL, userJWT);
        var list = filesClient.listFiles(EXEC_SYSTEM, "/", 100, 0, true);
        if (list == null) {
            System.out.println("Null list returned!");
        } else {
            System.out.println("Number of files returned: " + list.size());
            for (var f : list) {
                System.out.println("\nfile:  " + f.getName());
                System.out.println("  size:  " + f.getSize());
                System.out.println("  time:  " + f.getLastModified());
                System.out.println("  path:  " + f.getPath());
                System.out.println("  type:  " + f.getType());
                System.out.println("  owner: " + f.getOwner());
                System.out.println("  group: " + f.getGroup());
                System.out.println("  perms: " + f.getNativePermissions());
                System.out.println("  mime:  " + f.getMimeType());
            }
        }
    }
}
