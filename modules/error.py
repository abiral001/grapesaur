class Error:
    def showError(self):
        """Function to return the set error
        """
        if (self.error['status'] == True):
            print("Error {}, You can resolve the error by: {}".format(self.error['message'], self.error['resolution']))
        else:
            print("No errors reported")

    def setError(self, status):
        """Internal function that sets different error at different program execution step

        Args:
            status (int): Denotes the error code
        """
        if status == 0:
            self.error = {
                'status': False,
                'message': "No errors reported",
                'resolution': "Nothing to do"
            }
        elif status == 1:
            self.error = {
                'status': True,
                'message': "PySpark failed to initialize",
                'resolution': "Please check the install path for PySpark"
            }
        elif status == 2:
            self.error = {
                'status': True,
                'message': "No dataset provided",
                'resolution': "Please provide data using readFile(...)"
            }
        elif status == 3:
            self.error = {
                'status': True,
                'message': "Pyspark is not initialized",
                'resolution': "Please restart the application"
            }
        elif status == 4:
            self.error = {
                'status': True,
                'message': "File type not supported",
                'resolution': "Please contact developer for the required dataset type"
            }
        elif status == 5:
            self.error = {
                'status': True,
                'message': "File not found",
                'resolution': "Please place the file in the same directory as the main caller"
            }
        elif status == 6:
            self.error = {
                'status': True,
                'message': "Column name mismatch",
                'resolution': "Please check the column names in thr tree view"
            }
        elif status == 7:
            self.error = {
                'status': True,
                'message': "Parameter field name mismatch",
                'resolution': "Please recheck the entered parameter"
            }
        elif status == 8:
            self.error = {
                'status': True,
                'message': 'Only CSV files are supported for conversion',
                'resolution': 'Please supply a csv file'
            }