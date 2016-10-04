package error;

/**
 * 
 * This class is used to call custom error handing, in the project..  
 * We can print custom messages by using this.
 * 
 * @authors 
 * Saarthak Chandra - sc2776
 * Shweta Shrivastava - ss3646
 * Vikas P Nelamangala - vpn6
 */

public class SQLCustomErrorHandler extends Exception {
    public SQLCustomErrorHandler () {
    
    }
    
    /**
     * Calls the constructor of parent class Exception
	 * @param message
	 * 				Message sent for printing error
	 */
    public SQLCustomErrorHandler (String message) {
        super (message);
    }

    /**
     * Print the error class and reason to the sysOut 
     * 
     * @param cause
     * 				Here we print the cause of the error
     * @param message
     * 				Here we pass in the class name where the error is handled
     */
    
    public SQLCustomErrorHandler (Throwable cause,String message) {
        //System.out.println("Class is - "+ message);
        //System.out.println("Error is - "+ cause.getMessage());
    }

    /**
     * Call the super constructor , if we need to do so. 
     * 
     * @param cause
     * 				Here we print the cause of the error
     * @param message
     * 				Here we pass in the class name where the error is handled
     */
    public SQLCustomErrorHandler (String message, Throwable cause) {
        super (message, cause);
    }
}