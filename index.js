// Define CSV parser library
const csv = require('csv-parser')

// Define CSV writer library
const createCsvWriter = require('csv-writer').createObjectCsvWriter

// Define fs library
const fs = require('fs')

// This function is to lower case needed fields so it can be compared to each other later
// Example: "email" and "Email" key has to be compared for combining purpose
lowercaseObjectKey = (object) => {
  // Define new empty object
  let newObj = {}
  // Define the keys of the object in array format
  let objectKeys = Object.keys(object)
  // Iterate through the object keys array
  objectKeys.forEach((key) => {
    // Lower case each object key
    newObj[key.toLowerCase()] = object[key]
    // Check for email value
    if (newObj['email']) {
      // Lower case each email value so it can be compared
      newObj['email'] = newObj['email'].toLowerCase()
    }
  })
  // Return the modified object
  return newObj
}

// This function is to sanitize each object because there are some value that doesn't have key
// Example: on the file2.csv, there is one line consisting headerless value ("go","react","node.js")
sanitizedObject = (object) => {
  // Check if object has extra value that don't have key
  if (object._5) {
    // Build an array consisting of all "skills" value (including the un-keyed value)
    let array = [object.skills, object._5, object._6, object._7]
    // Assign joined skills into the previous object
    object.skills = array.join(',')
    // Clean the object from previously added un-keyed value because we already store the value inside "skills" key on the previous step
    delete object._5
    delete object._6
    delete object._7
  }
}

// This function is to read and parse the CSV into JSON
parseCsv = (file) => {
  // Make this function as a promise (async purpose)
  return new Promise ((resolve, reject) => {
    // Define empty array to store parsed csv
    let parsed = []
    // Start the reading function from fs library
    fs.createReadStream(file)
    .pipe(csv())
    .on('data', (row) => {
      // Push each readed row into the empty array
      parsed.push(row)
    })
    .on('end', () => {
      // Resolve the promise on reading end
      resolve(parsed)
    })
    .on('error', (err) => reject(err)) // Reject on error
  })
}

// This function is to combine the object using common field
// Example: "email" field as the unique common field
combineCsv = (data, commonField) => {
  // Create new Map object to store key value pair (for checking purpose)
  let newCsvData = new Map()
  // Iterate through each object that were given
  data.forEach((eachObj) => {
    // Define property (common field) that we are going to use as the object unique identification
    const property = eachObj[commonField]
    // Check if the Map object already containing common field property
    if (newCsvData.has(property)) {
      // If yes, then update the Map value with the other values
      newCsvData.set(property, {...eachObj, ...newCsvData.get(property)})
    } else {
      // If not, then create new Map property and value
      newCsvData.set(property, eachObj)
    }
  })
  // Convert the Map objects into regular objects and build an array consisting those objects
  return Array.from(newCsvData.values())
}

// This function is to read and convert the CSV
readCsv = (files) => {
  // Make this function as a promise (async purpose)
  return new Promise ((resolve, reject) => {
    // Define array to store promises
    let promises = []
    // Define array to store data from parsed CSV
    let arrObj = []
    // Iterate through array of files defined on the parameter
    files.forEach((file) => {
      // Call the parse CSV function and pushed it into array of promises
      promises.push(parseCsv(file))
    })
    // Do promise all to read all the files simultaniously
    Promise
    .all(promises)
    .then((results) => {
      // Iterate through each promise results
      results.forEach((datas) => {
        // Iterate through each data resulting from parsed CSV
        datas.forEach((data, i) => {
          // Use the lowercaseObjectKey function to lower case each data (for comparing purpose)
          let newData = lowercaseObjectKey(data)
          // Sanitize each data using this function
          sanitizedObject(newData)
          // Push the santized and case lowered data into the array
          arrObj.push(newData)
        })
      })
      // After we finished all the reading and converting, now we combined the CSV using this function
      resolve(combineCsv(arrObj, 'email'))
    })
    .catch((err) => reject(err))
  })
}

// This function is to write back the clean and combined object back into CSV
writeCsv = (data) => {
  // Make this function as a promise (async purpose)
  return new Promise ((resolve, reject) => {
    // Define writer function using csv-writer library
    const csvWriter = createCsvWriter({
      path: 'out.csv', // Define the path of the written file
      header: [ // Define the header properties
        {id: 'email', title: 'email'},
        {id: 'name', title: 'name'},
        {id: 'profile_id', title: 'profile_id'},
        {id: 'total_exp', title: 'total_exp'},
        {id: 'highest qualification', title: 'highest qualification'},
        {id: 'company_name', title: 'company_name'},
        {id: 'skills', title: 'skills'},
        {id: 'created_at', title: 'created_at'},
      ]
    })
    // Start write the data
    csvWriter
    .writeRecords(data)
    .then(() => resolve('success!'))
    .catch((err) => reject(err))
  })
}

// Read the CSV
// How to use: readCsv(['FILE_NAME_ONE','FILE_NAME_TWO])
readCsv(['file1.csv', 'file2.csv']).then((result) => {
  // After the reading, converting, and combining is done, call the write csv function to write back the combined csv
  writeCsv(result)
  .then((success) => console.log(success))
  .catch((err) => console.log(err))
})