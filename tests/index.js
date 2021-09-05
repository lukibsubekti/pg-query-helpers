const tests = {
  db: require('./db'),
}

function countTests() {
  let counter = 0;

  for (let key in tests) {
    if (tests.hasOwnProperty(key)) {
      for (let testKey in tests[key]) {
        if (tests[key].hasOwnProperty(testKey)) {
          counter++;
        }
      }
    }
  }

  return counter;
}

function produceReport(limit, successes, errors) {
  console.log('');
  console.log('----- TEST REPORTS -----');
  console.log('');
  console.log('\x1b[33m%s\x1b[0m', `Total: ${limit}`);
  console.log('\x1b[32m%s\x1b[0m', `Success: ${successes}`);
  console.log('\x1b[31m%s\x1b[0m', `Error: ${errors.length}`);
  console.log('');

  if (errors.length > 0) {
    console.log('');
    console.log('----- Error Details -----');
    console.log('');

    errors.forEach(err => {
      console.log('\x1b[31m%s\x1b[0m', `Name: ${err.name}`);
      console.log('Error:', err.error);
    });
  }

  // finish the app
  process.exit(0);
}

function runTests() {
  let limit = countTests();
  let counter = 0;
  let successes = 0;
  let errors = [];

  for (let key in tests) {
    if (tests.hasOwnProperty(key)) {
      for (let testName in tests[key]) {
        if (tests[key].hasOwnProperty(testName)) {
          (function(){
            let tmpTestName = testName;
            let test = tests[key][testName];

            try {
              test(function(){
                console.log('\x1b[32m%s\x1b[0m', tmpTestName);
                counter++;
                successes++;
                if (counter===limit) {
                  produceReport(limit, successes, errors);
                }
              });
            } catch (error) {
              console.log('\x1b[31m%s\x1b[0m', tmpTestName);
              counter++;
              errors.push({
                name: tmpTestName,
                error: error
              });
              if (counter===limit) {
                produceReport(limit, successes, errors);
              }
            }
          })();
        }
      }
    }
  }
}

runTests();