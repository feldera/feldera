import Editors from 'src/analytics/editor'

const Editor = () => {
  // Create a new empty project
  return (
    <Editors
      program={{
        project_id: null,
        name: '',
        description: '',
        status: 'None',
        version: 0,
        code: ''
      }}
    />
  )
}

export default Editor
