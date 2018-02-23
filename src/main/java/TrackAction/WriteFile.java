package TrackAction;


import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.Tuple;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.ComposeRequest;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.Storage.SignUrlOption;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;

import java.io.FileInputStream;

public class WriteFile {
    public void writeFile() throws Exception{
        // Create your service object
        //Storage storage = StorageOptions.getDefaultInstance().getService();

// Create a bucket
        String bucketName = "aaaaa_bucket"; // Change this to something unique
        //Bucket bucket = storage.create(BucketInfo.of(bucketName));

// Upload a blob to the newly created bucket
        //BlobId blobId = BlobId.of(bucketName, "blob");
        //BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
        //Blob blob = storage.create(blobInfo, "a simple blob".getBytes(UTF_8),
        //        BlobTargetOption.predefinedAcl(Storage.PredefinedAcl.PUBLIC_READ_WRITE));

        //storage.create(blobInfo,
        //        "Hello, ACL!".getBytes(),
       //         BlobTargetOption.predefinedAcl(Storage.PredefinedAcl.PUBLIC_READ));
        //copyTo(BlobId.of(targetBucket, targetBlob), options);
        //Storage storage = StorageOptions.getDefaultInstance().getService();
        //BlobId blobId = BlobId.of(bucketName, "blob_name");
        //BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
        //Blob blob = storage.create(blobInfo, "Hello, Cloud Storage!".getBytes(UTF_8));

        Storage storage = StorageOptions.newBuilder()
                .setProjectId("umg-dev")
                .setCredentials(GoogleCredentials.fromStream(new FileInputStream("/Users/rudenka/Downloads/UMGDevCreds.json")))
                .build()
                .getService();
        BlobId blobId = BlobId.of(bucketName, "blob_name");
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
        Blob blob = storage.create(blobInfo, "Hello, Cloud Storage!".getBytes(UTF_8), Storage.BlobTargetOption.predefinedAcl(Storage.PredefinedAcl.PUBLIC_READ_WRITE));
        //Blob blob = storage.create("aaaaa_bucket","sssss".getBytes(), Storage.BlobTargetOption.userProject("umg-dev"));
        //String fileContent = new String(blob.getContent());

    }
    public static void main(String args[]) throws Exception{
        WriteFile mdb = new WriteFile();
        mdb.writeFile();
    }

}
